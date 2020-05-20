package proxy

import (
	"context"
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"net"
	"net/rpc"
)

type ServiceClient struct {
	master               string
	addr                 string
	callbackListener     net.Listener
	logger               utils.Logger
	closeResponseChannel bool
	closed               chan interface{}
	reply                chan *rpcInterfaces.ResponseCallback
}

func (client *ServiceClient) OnClose() <-chan interface{} {
	return client.closed
}

func (client *ServiceClient) OnReply() <-chan *rpcInterfaces.ResponseCallback {
	return client.reply
}

func (client *ServiceClient) Callback(args *rpcInterfaces.ResponseCallback, reply *rpcInterfaces.NoopReply) error {
	_, closer := args.CreateServerSpan("callback")
	defer closer()
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
		master:               master,
		addr:                 addr,
		callbackListener:     nil,
		logger:               logger,
		closeResponseChannel: false,
		reply:                make(chan *rpcInterfaces.ResponseCallback),
		closed:               make(chan interface{}),
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

func (client *ServiceClient) Query(context context.Context, key string) {
	span, queryCtx := opentracing.StartSpanFromContext(context, "query-start")
	defer span.Finish()
	req := rpcInterfaces.ChainConfigurationRequest{
		RequestBase: utils.NewRequestBase(),
	}
	var reply rpcInterfaces.ChainConfigurationReply
	err := utils.TraceableCall(client.master, "Master.GetChainConfiguration", &req, &reply, queryCtx)
	if err != nil {
		client.logger.Error().Err(err).Msgf("Failed to fetch configuration from {0}", client.master)
	}
	qr := rpcInterfaces.QueryArgs{
		Key:          key,
		ReplyAddress: client.addr,
		RequestBase:  utils.NewRequestBase(),
	}

	var qreply rpcInterfaces.QueryReply

	client.logger.Info().Msgf("Master Reply -> Tail is %s, Head is %s", reply.Tail, reply.Head)
	resp := rpcInterfaces.ResponseCallback{
		RequestBase: utils.NewRequestBase(),
	}
	resp.RequestId = qr.RequestId
	if reply.Tail != "" {
		ext.PeerService.Set(span, "proxy")
		err := utils.TraceableCall(reply.Tail, "ChainReplicationNode.Query", &qr, &qreply, queryCtx)
		resp.Key = qreply.Key
		resp.Value = qreply.Value

		if err != nil {
			client.logger.Error().Err(err)
			resp.Error = err.Error()
		} else if !qreply.Ok {
			resp.Error = "unexpected error"
		}
	} else {
		resp.Error = "not ready"
	}

	go func(callback rpcInterfaces.ResponseCallback) {
		client.reply <- &callback
	}(resp)
}

func (client *ServiceClient) Update(context context.Context, key string, value string) {
	span, updateCtx := opentracing.StartSpanFromContext(context, "update-rpc")
	defer span.Finish()
	req := rpcInterfaces.ChainConfigurationRequest {
		RequestBase : utils.NewRequestBase(),
	}
	var reply rpcInterfaces.ChainConfigurationReply
	utils.TraceableCall(client.master, "Master.GetChainConfiguration", &req, &reply, updateCtx)
	updateArg := rpcInterfaces.UpdateArgs{
		Key:          key,
		RequestBase:  utils.NewRequestBase(),
		ReplyAddress: client.addr,
		Value:        value,
	}

	var updateReply rpcInterfaces.UpdateReply

	if reply.Head != "" {
		utils.TraceableCall(reply.Head, "ChainReplicationNode.Update", &updateArg, &updateReply, updateCtx)
		client.logger.Info().Msgf("Update: %v, sent successfully.", updateArg)
	} else {
		span.LogFields(log.String("event", "update failure"), log.String("reason", "service unavailable"))
		client.logger.Error().Msgf("Service %s not ready yet, Update (Key = %s, Value = %s) not sent", client.master, key, value)
	}
}

func (client *ServiceClient) Shutdown() {
	client.closeResponseChannel = true
	client.callbackListener.Close()
}
