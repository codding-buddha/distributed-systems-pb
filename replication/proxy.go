package replication

import (
	"github.com/codding-buddha/ds-pb/utils"
	guuid "github.com/google/uuid"
	"net"
	"net/rpc"
)

type ServiceClient struct {
	master string
	addr string
	callbackListener net.Listener
	logger utils.Logger
	closeResponseChannel bool
	closed chan interface{}
}

func(client *ServiceClient) OnClose() <- chan interface{}{
	return client.closed
}

func (client *ServiceClient) Callback(args *ResponseCallback, reply *NoopReply) error {
	client.logger.Info().Msg("Got reply callback")
	if args.Error == "" {
		client.logger.
			Info().
			Msgf("Received reply for request %v, Key: %s, Value: %s", args.RequestId, args.Key, args.Value)
	} else {
		client.logger.
			Error().
			Msgf("Request %v failed with error %s", args.RequestId, args.Error)
	}

	return nil
}

func InitClient(addr string, master string, logger utils.Logger) *ServiceClient {
	client := &ServiceClient{
		master:           master,
		addr:             addr,
		callbackListener: nil,
		logger:           logger,
		closeResponseChannel: false,
	}

	rpcs := rpc.NewServer()
	rpcs.Register(client)
	l, err := net.Listen("tcp", client.addr)
	if err != nil {
		client.logger.Fatal().Err(err).Msg("Failed to create callback channel")
		return client
	}

	client.callbackListener = l
	go func() {
		for !client.closeResponseChannel {
			conn, err := client.callbackListener.Accept()

			if !client.closeResponseChannel && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if client.closeResponseChannel == true && err != nil {
				client.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()

	return client
}


func (client *ServiceClient) Query(key string) {
	req := ChainConfigurationRequest{}
	var reply ChainConfigurationReply
	utils.Call(client.master, "Master.GetChainConfiguration", &req, &reply)
	qr := QueryArgs{
		Key:          key,
		RequestId:    guuid.New().String(),
		ReplyAddress: client.addr,
	}

	var qreply QueryReply

	if reply.Tail != "" {
		utils.Call(reply.Tail, "ChainNode.Query", &qr, &qreply)
		client.logger.Info().Msgf("Query: %v, sent successfully.", qr)
	} else {
		client.logger.Error().Msgf("Service %s not ready yet, Query (Key = %s) not sent", client.master, key)
	}
}

func (client *ServiceClient) Update(key string, value string) {
	req := ChainConfigurationRequest{}
	var reply ChainConfigurationReply
	utils.Call(client.master, "Master.GetChainConfiguration", &req, &reply)
	updateArg := UpdateArgs{
		Key:          key,
		RequestId:    guuid.New().String(),
		ReplyAddress: client.addr,
		Value: value,
	}

	var updateReply UpdateReply

	if reply.Head != "" {
		utils.Call(reply.Head, "ChainNode.Update", &updateArg, &updateReply)
		client.logger.Info().Msgf("Update: %v, sent successfully.", updateArg)
	} else {
		client.logger.Error().Msgf("Service %s not ready yet, Update (Key = %s, Value = %s) not sent", client.master, key, value)
	}
}