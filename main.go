package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
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
	Master    = "master"
	Client    = "client"
)

func main() {
	mode := flag.String("mode", Master, "Mode")
	addr := flag.String("addr", "localhost:9000", "Address of service")
	masterAddr := flag.String("maddr", "localhost:9000", "Address of master")
	flag.Parse()
	logger := utils.NewConsole(true)
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)

	if *mode == Master {
		tracer, closer := utils.InitJaeger("master")
		defer closer.Close()
		opentracing.SetGlobalTracer(tracer)
		master.StartMaster(*addr, *logger)
		<-c
	} else if *mode == Client {
		tracer, closer := utils.InitJaeger("client")
		defer closer.Close()
		opentracing.SetGlobalTracer(tracer)
		client, _ := proxy.InitClient(*addr, *masterAddr, *logger)
		scanner := bufio.NewScanner(os.Stdin)
		reply := client.OnReply()
		for scanner.Scan() {
			input := scanner.Text()
			if input == "q" {
				client.Shutdown()
			}

			parts := strings.Split(input, " ")

			if parts[0] == "q" {
				span := tracer.StartSpan("query")
				ctx := opentracing.ContextWithSpan(context.Background(), span)
				client.Query(ctx, parts[1])
				response := <- reply
				span.Finish()
				logger.Printf("Key : %s, Value : %s", response.Key, response.Value)
			} else {
				span := tracer.StartSpan("update")
				ctx := opentracing.ContextWithSpan(context.Background(), span)
				client.Update(ctx, parts[1], parts[2])
				response := <- reply
				span.Finish()
				logger.Printf("Key : %s, Value : %s", response.Key, response.Value)
			}
		}

		if scanner.Err() != nil {
			client.Shutdown()
		}

	} else {
		tracer, closer := utils.InitJaeger("chain-node:" + *addr)
		defer closer.Close()
		opentracing.SetGlobalTracer(tracer)
		store, _ := storage.New(logger, *addr)
		node := node.NewChainReplicationNode(*addr, store, *masterAddr, *logger)
		node.Start()
		select {
		case <-c:
			logger.Info().Msg("Shutting down")
			node.Shutdown()
		case <-node.OnClose():
			logger.Info().Msg("Node killed")
		}
	}
}

func testLookupService() {
	addr := "localhost:9000"
	mode := flag.String("mode", "server", "Mode")
	flag.Parse()
	logger := utils.NewConsole(true)
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
