package main

import (
	"flag"
	"fmt"
	"github.com/codding-buddha/ds-pb/kv_store"
	"github.com/codding-buddha/ds-pb/log"
	"net/rpc"
)

func main()  {
	addr := "localhost:9000"
	mode := flag.String("mode", "server", "Mode")
	flag.Parse()
	logger := log.NewConsole(true)
	if *mode == "server" {
		store, err := kv_store.New(logger)
		if err != nil {
			logger.Printf("Service init failed %v\n", err)
			return
		}

		lookupService := kv_store.Run(store, addr, logger)
		<- lookupService.OnClose()
	} else {
		logger.Printf("Sending client requests")
		args := kv_store.WriteRecordArgs{Record: &kv_store.Record{
			Key:   "key1",
			Value: "Value1",
		}}
		var reply kv_store.WriteRecordReply
		ok := call(addr, "LookupService.Write", args, &reply)

		if ok {
			getReq := kv_store.GetRecordArgs{Key: "key1"}
			var getReply kv_store.GetRecordReply
			ok := call(addr, "LookupService.Get", getReq, &getReply)

			if ok {
				logger.Printf("Got reply key: %s, value: %s", getReply.Record.Key, getReply.Record.Value)
			} else {
				logger.Error().Msg("Get request failed")
			}
		} else {
			logger.Error().Msg("Write request failed")
		}

		var srep kv_store.ShutdownServiceReply
		call(addr, "LookupService.Kill", kv_store.ShutdownServiceArgs{}, &srep)
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
