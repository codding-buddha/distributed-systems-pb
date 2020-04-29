package main

import (
	"flag"
	"fmt"
	"github.com/codding-buddha/ds-pb/replication"
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	"net/rpc"
	"os"
	"os/signal"
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
	signal.Notify(c, os.Interrupt)

	if *mode == Master {
		replication.StartMaster(*addr, *logger)
	} else if *mode == Client {
		client := replication.InitClient(*addr, *masterAddr, *logger)
		client.Update("a", "26")
		client.Query("b")
		client.Update("b", "27")
		client.Query("a")
		client.Update("a", "29")
		client.Query("a")
		client.Query("b")
		<-c
	} else {
		store, _ := storage.New(logger)
		node := replication.NewChainNode(*addr, store, *masterAddr, *logger)
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
		store, err := storage.New(logger)
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