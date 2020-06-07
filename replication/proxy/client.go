package proxy

import (
	"context"
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/opentracing/opentracing-go"
	"net"
	"net/rpc"
	"strconv"
)

type ServiceClient struct {
	master               string
	addr                 string
	callbackListener     net.Listener
	logger               utils.Logger
	closeResponseChannel bool
	closed               chan interface{}
	requestCounter       int
	dispatchMode         bool
	callBacks            map[string]chan<- *Result
}

type Result struct {
	Key   string
	Value string
	Error string
}

func (client *ServiceClient) OnClose() <-chan interface{} {
	return client.closed
}

func (client *ServiceClient) Callback(args *rpcInterfaces.ResponseCallback, reply *rpcInterfaces.NoopReply) error {
	_, closer := args.CreateServerSpan("callback")
	defer closer()
	client.logger.Info().Msg("Got reply callback")
	n, ok := client.callBacks[args.RequestId]
	if ok {
		delete(client.callBacks, args.RequestId)
		go func() {
			result := Result{
				Key:   args.Key,
				Value: args.Value,
				Error: args.Error,
			}
			n <- &result
		}()
	}
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

func New(addr string, master string, mode string, logger utils.Logger) (*ServiceClient, error) {

	client := &ServiceClient{
		master:               master,
		dispatchMode:         mode == "dispatcher",
		addr:                 addr,
		callbackListener:     nil,
		logger:               logger,
		closeResponseChannel: false,
		closed:               make(chan interface{}),
		requestCounter:       1,
		callBacks:            make(map[string]chan<- *Result),
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

func (client *ServiceClient) getQueryAddr(context context.Context) (string, string) {
	if client.dispatchMode {
		return client.master, "Dispatcher.Query"
	}

	req := rpcInterfaces.ChainConfigurationRequest{
		RequestBase: utils.NewRequestBase(),
	}
	var reply rpcInterfaces.ChainConfigurationReply
	err := utils.TraceableCall(client.master, "Master.GetChainConfiguration", &req, &reply, context)
	if err != nil {
		client.logger.Error().Err(err).Msgf("Failed to fetch configuration from {0}", client.master)
	}
	return reply.Tail, "ChainReplicationNode.Query"
}

func (client *ServiceClient) getUpdateAddr(context context.Context) (string, string) {
	if client.dispatchMode {
		return client.master, "Dispatcher.Update"
	}

	req := rpcInterfaces.ChainConfigurationRequest{
		RequestBase: utils.NewRequestBase(),
	}
	var reply rpcInterfaces.ChainConfigurationReply
	err := utils.TraceableCall(client.master, "Master.GetChainConfiguration", &req, &reply, context)
	if err != nil {
		client.logger.Error().Err(err).Msgf("Failed to fetch configuration from {0}", client.master)
	}
	return reply.Head, "ChainReplicationNode.Update"
}

func (client *ServiceClient) Query(context context.Context, key string) Result {
	span, queryCtx := opentracing.StartSpanFromContext(context, "query-start")
	defer span.Finish()

	qr := rpcInterfaces.QueryArgs{
		Key:          key,
		ReplyAddress: client.addr,
		RequestBase:  utils.NewRequestBase(),
	}

	var qreply rpcInterfaces.QueryReply
	srv, rpcname := client.getQueryAddr(queryCtx)
	error := utils.TraceableCall(srv, rpcname, &qr, &qreply, queryCtx)
	result := Result{
		Key:   key,
		Value: "",
		Error: "",
	}

	if error != nil {
		result.Error = error.Error()
	}

	if error == nil && qreply.Ok {
		result.Value = qreply.Value
	}

	if error == nil && !qreply.Ok {
		result.Error = "unexpected error"
	}

	return result
}

func (client *ServiceClient) Update(context context.Context, key string, value string, notify chan<- *Result) error {
	span, updateCtx := opentracing.StartSpanFromContext(context, "update-rpc")
	defer span.Finish()
	srv, rpcname := client.getUpdateAddr(updateCtx)
	updateArg := rpcInterfaces.UpdateArgs{
		Key:          key,
		RequestBase:  utils.NewRequestBase(),
		ReplyAddress: client.addr,
		Value:        value,
	}
	updateArg.RequestId = client.addr + "-req-" + strconv.Itoa(client.requestCounter)
	client.requestCounter++

	var updateReply rpcInterfaces.UpdateReply
	error := utils.TraceableCall(srv, rpcname, &updateArg, &updateReply, updateCtx)

	if error == nil {
		client.callBacks[updateArg.RequestId] = notify
	}
	return error
}


func (client *ServiceClient) Shutdown() {
	client.closeResponseChannel = true
	client.callbackListener.Close()
}
