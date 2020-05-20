package utils

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"net/rpc"
)

func Call(srv string, rpcname string, args interface{}, reply interface{}, ctx ...context.Context) error {
	if len(ctx) > 0 {
		contxt := ctx[0]
		span, _ := opentracing.StartSpanFromContext(contxt, rpcname)
		ext.SpanKindRPCClient.Set(span)
		span.SetTag("rpc.service", srv)
		defer span.Finish()
	}

	connection, err := rpc.Dial("tcp", srv)
	if err != nil {
		return err
	}

	defer connection.Close()

	err = connection.Call(rpcname, args, reply)
	return err
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

	connection, err := rpc.Dial("tcp", srv)
	if err != nil {
		return err
	}

	defer connection.Close()

	err = connection.Call(rpcname, args, reply)
	return err
}