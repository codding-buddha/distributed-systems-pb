package proxy

import (
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
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
	reply chan *rpcInterfaces.ResponseCallback
}

func(client *ServiceClient) OnClose() <- chan interface{}{
	return client.closed
}

func (client *ServiceClient) OnReply() <- chan *rpcInterfaces.ResponseCallback {
	return client.reply
}

func (client *ServiceClient) Callback(args *rpcInterfaces.ResponseCallback, reply *rpcInterfaces.NoopReply) error {
	client.logger.Info().Msg("Got reply callback")
	client.reply <- args
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

func InitClient(addr string, master string, logger utils.Logger) (*ServiceClient, error) {
	client := &ServiceClient{
		master:           master,
		addr:             addr,
		callbackListener: nil,
		logger:           logger,
		closeResponseChannel: false,
		reply: make(chan *rpcInterfaces.ResponseCallback),
		closed: make(chan interface{}),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(client)
	l, err := net.Listen("tcp", client.addr)
	if err != nil {
		client.logger.Fatal().Err(err).Msg("Failed to create callback channel")
		return client, err
	}

	client.callbackListener = l
	go func() {
		for !client.closeResponseChannel {
			conn, err := client.callbackListener.Accept()

			if !client.closeResponseChannel && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if !client.closeResponseChannel && err != nil {
				client.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()

	return client, nil
}


func (client *ServiceClient) Query(key string) {
	req := rpcInterfaces.ChainConfigurationRequest{}
	var reply rpcInterfaces.ChainConfigurationReply
	err := utils.Call(client.master, "Master.GetChainConfiguration", &req, &reply)
	if err != nil {
		client.logger.Error().Err(err).Msgf("Failed to fetch configuration from {0}", client.master)
	}
	qr := rpcInterfaces.QueryArgs{
		Key:          key,
		RequestId:    guuid.New().String(),
		ReplyAddress: client.addr,
	}

	var qreply rpcInterfaces.QueryReply

	client.logger.Info().Msgf("Master Reply -> Tail is %s, Head is %s", reply.Tail, reply.Head)
	if reply.Tail != "" {
		utils.Call(reply.Tail, "ChainReplicationNode.Query", &qr, &qreply)
		client.logger.Info().Msgf("Query: %v, sent successfully.", qr)
	} else {
		client.logger.Error().Msgf("Service %s not ready yet, Query (Key = %s) not sent", client.master, key)
	}
}

func (client *ServiceClient) Update(key string, value string) {
	req := rpcInterfaces.ChainConfigurationRequest{}
	var reply rpcInterfaces.ChainConfigurationReply
	utils.Call(client.master, "Master.GetChainConfiguration", &req, &reply)
	updateArg := rpcInterfaces.UpdateArgs{
		Key:          key,
		RequestId:    guuid.New().String(),
		ReplyAddress: client.addr,
		Value: value,
	}

	var updateReply rpcInterfaces.UpdateReply

	if reply.Head != "" {
		utils.Call(reply.Head, "ChainReplicationNode.Update", &updateArg, &updateReply)
		client.logger.Info().Msgf("Update: %v, sent successfully.", updateArg)
	} else {
		client.logger.Error().Msgf("Service %s not ready yet, Update (Key = %s, Value = %s) not sent", client.master, key, value)
	}
}

func (client *ServiceClient) Shutdown() {
	client.closeResponseChannel = true
	client.callbackListener.Close()
}