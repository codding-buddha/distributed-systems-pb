package utils

import (
	"fmt"
	guuid "github.com/google/uuid"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	jaeger "github.com/uber/jaeger-client-go"
	config "github.com/uber/jaeger-client-go/config"
	"io"
)

// initJaeger returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func InitJaeger(service string) (opentracing.Tracer, io.Closer) {
	cfg := &config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
		ServiceName: service,
	}
	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}

	return tracer, closer
}

type RequestArgs interface {
	GetContext() *RequestContext
	Id() string
}

type RequestContext struct {
	Context map[string]string
}

type RequestContextCarrier RequestContext

// Set implements ForeachKey() of opentracing.TextMapWriter
func (ctx RequestContextCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, v := range ctx.Context {
		if err := handler(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Set implements Set() of opentracing.TextMapWriter
func (ctx RequestContextCarrier) Set(key, val string) {
	ctx.Context[key] = val
}

type RequestBase struct {
	Ctx       RequestContext
	RequestId string
}

func (r *RequestBase) GetContext() *RequestContext {
	return &r.Ctx
}

func (r *RequestBase) Id() string {
	return r.RequestId
}

func (r *RequestBase) CreateServerSpan(operationName string, configurations ...func(span opentracing.Span)) (opentracing.Span, func()) {
	tracer := opentracing.GlobalTracer()
	spanCtx, _ := tracer.Extract(opentracing.TextMap, RequestContextCarrier(*r.GetContext()))
	span := tracer.StartSpan(operationName, ext.RPCServerOption(spanCtx))
	span.SetTag("request-id", r.RequestId)
	if len(configurations) > 0 {
		for _, configure := range configurations {
			configure(span)
		}
	}
	return span, func() {
		span.Finish()
	}
}

func NewRequestBase() RequestBase {
	reqId, _ := guuid.NewUUID()
	return RequestBase{
		Ctx:       RequestContext{
			Context: make(map[string]string),
		},
		RequestId: reqId.String(),
	}
}