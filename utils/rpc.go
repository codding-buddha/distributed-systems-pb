package utils

import (
	"context"
	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"net"
	"net/rpc"
	"time"
)

func Call(srv string, rpcname string, args interface{}, reply interface{}, ctx ...context.Context) error {
	if len(ctx) > 0 {
		contxt := ctx[0]
		span, _ := opentracing.StartSpanFromContext(contxt, rpcname)
		ext.SpanKindRPCClient.Set(span)
		span.SetTag("rpc.service", srv)
		defer span.Finish()
	}

	retryCount := 3
	for ; retryCount > 0; retryCount-- {
		connection, err := net.DialTimeout("tcp", srv, time.Second*30)
		client := rpc.NewClient(connection)
		err = client.Call(rpcname, args, reply)
		client.Close()
		connection.Close()
		if err == nil {
			return err
		}
	}

	return errors.Timeoutf("timeout: %s, %s ", srv, rpcname)
}

func TraceableCall(srv string, rpcname string, args interface{}, reply interface{}, ctx ...context.Context) error {
	if len(ctx) > 0 {
		reqCtx := args.(RequestArgs)
		contxt := ctx[0]
		span, _ := opentracing.StartSpanFromContext(contxt, rpcname)
		ext.SpanKindRPCClient.Set(span)
		span.SetTag("rpc.service", srv)
		span.Tracer().Inject(span.Context(), opentracing.TextMap, RequestContextCarrier(*reqCtx.GetContext()))
		defer span.Finish()
	}

	retryCount := 3
	for ; retryCount > 0; retryCount-- {
		connection, err := net.DialTimeout("tcp", srv, time.Second*30)
		client := rpc.NewClient(connection)
		err = client.Call(rpcname, args, reply)
		client.Close()
		connection.Close()
		if err == nil {
			return err
		}
	}

	return errors.Timeoutf("timeout: %s, %s ", srv, rpcname)
}