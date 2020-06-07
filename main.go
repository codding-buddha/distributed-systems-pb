package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/codding-buddha/ds-pb/experiments"
	"github.com/codding-buddha/ds-pb/replication/master"
	"github.com/codding-buddha/ds-pb/replication/node"
	"github.com/codding-buddha/ds-pb/replication/proxy"
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/opentracing/opentracing-go"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
)

const (
	Master     = "master"
	Client     = "client"
	Chain      = "chain"
	Experiment = "experiment"
)

func main() {
	mode := flag.String("mode", Master, "Mode")
	addr := flag.String("addr", "localhost:9000", "Address of service")
	masterAddr := flag.String("maddr", "localhost:9000", "Address of master")
	experiment := flag.String("experiment", "cr-throughput", "Experiment to run")
	debug := flag.Bool("debug", false, "Debug mode")
	chainMode := flag.String("chainMode", "strong", "Use 'weak' for weak consistency")
	trace := flag.Bool("enableTrace", true, "Use false to disable tracing")
	flag.Parse()
	logger := utils.NewConsole(*debug, *addr)
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)

	if *mode == Master {
		tracer, closer := utils.InitJaeger("master", !*trace)
		defer closer.Close()
		opentracing.SetGlobalTracer(tracer)
		master.StartMaster(*addr, *logger, *chainMode == "weak")
		<-c
	} else if *mode == Client {
		tracer, closer := utils.InitJaeger("client", !*trace)
		defer closer.Close()
		opentracing.SetGlobalTracer(tracer)
		client, _ := proxy.New(*addr, *masterAddr, "master", *logger)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			input := scanner.Text()
			if input == "q" {
				client.Shutdown()
			}

			parts := strings.Split(input, " ")

			if parts[0] == "q" {
				span := tracer.StartSpan("query")
				ctx := opentracing.ContextWithSpan(context.Background(), span)
				response := client.Query(ctx, parts[1])
				span.Finish()
				logger.Printf("Key : %s, Value : %s", response.Key, response.Value)
			} else {
				reply := make(chan *proxy.Result)
				span := tracer.StartSpan("update")
				ctx := opentracing.ContextWithSpan(context.Background(), span)
				client.Update(ctx, parts[1], parts[2], reply)
				response := <-reply
				span.Finish()
				logger.Printf("Key : %s, Value : %s", response.Key, response.Value)
			}
		}

		if scanner.Err() != nil {
			client.Shutdown()
		}

	} else if *mode == Chain {
		tracer, closer := utils.InitJaeger("chain-node:" + *addr, !*trace)
		defer closer.Close()
		opentracing.SetGlobalTracer(tracer)
		store, _ := storage.New(logger, *addr)
		var n node.ChainReplicationNode
		if *chainMode == "strong" {
			n = node.NewChainReplicationNode(*addr, store, *masterAddr, *logger)
		} else {
			n = node.NewWeakChainReplicationNode(*addr, store, *masterAddr, *logger)
		}

		n.Start()
		select {
		case <-c:
			logger.Info().Msg("Shutting down")
			n.Shutdown()
		case <-n.OnClose():
			logger.Info().Msg("Node killed")
		}
	} else if *mode == Experiment {
		if *experiment == "cr-throughput" {
			experiments.UpdateAndQueryThroughput(*addr, *masterAddr)
		}
		<-c
	} else {
		logger.Error().Msgf("Invalid mode")
	}
}

func testLookupService() {
	addr := "localhost:9000"
	mode := flag.String("mode", "server", "Mode")
	flag.Parse()
	logger := utils.NewConsole(true, "client")
	if *mode == "server" {
		store, err := storage.New(logger, "lookup.db")
		if err != nil {
			logger.Printf("Service init failed %v\n", err)
			return
		}

		lookupService := storage.Run(store, addr, logger)
		<-lookupService.OnClose()
	} else {
		logger.Printf("Sending client requests")
		args := storage.WriteRecordArgs{Record: &storage.Record{
			Key:   "key1",
			Value: "Value1",
		}}
		var reply storage.WriteRecordReply
		ok := call(addr, "LookupService.Write", args, &reply)

		if ok {
			getReq := storage.GetRecordArgs{Key: "key1"}
			var getReply storage.GetRecordReply
			ok := call(addr, "LookupService.Get", getReq, &getReply)

			if ok {
				logger.Printf("Got reply key: %s, value: %s", getReply.Record.Key, getReply.Record.Value)
			} else {
				logger.Error().Msg("Get request failed")
			}
		} else {
			logger.Error().Msg("Write request failed")
		}

		var srep storage.ShutdownServiceReply
		call(addr, "LookupService.Kill", storage.ShutdownServiceArgs{}, &srep)
	}
}

func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	connection, err := rpc.Dial("tcp", srv)
	if err != nil {
		return false
	}

	defer connection.Close()

	err = connection.Call(rpcname, args, reply)

	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
