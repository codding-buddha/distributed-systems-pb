package utils

import (
	"net/rpc"
)

func Call(srv string, rpcname string, args interface{}, reply interface{}) error {
	connection, err := rpc.Dial("tcp", srv)
	if err != nil {
		return err
	}

	defer connection.Close()

	err = connection.Call(rpcname, args, reply)
	return err
}