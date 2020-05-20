package node

import (
	"context"
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
)

func (node *ChainReplicationNode) Update(request *rpcInterfaces.UpdateArgs, reply *rpcInterfaces.UpdateReply) error {
	span, closer := request.CreateServerSpan("update", node.populateSpan)
	defer closer()

	node.logger.Printf("Update request ++")
	reply.Ok = false
	if !node.CanServeRequest() {
		node.logger.Printf("Unable to service request currently")
		return errors.NotSupportedf("Unable to service request currently")
	}

	if !node.CanServeUpdate() {
		node.logger.Printf("Cannot service update request")
		return errors.BadRequestf("Cannot service update request")
	}

	req := Request{
		id:          request.RequestId,
		requestType: Update,
		key:         request.Key,
		value:       request.Value,
		replyAddr:   request.ReplyAddress,
		ctx:         span.Context(),
	}

	node.queueRequest(opentracing.ContextWithSpan(context.Background(), span), &req)
	node.logger.Printf("Update request --")
	reply.Ok = true
	return nil
}

func (node *ChainReplicationNode) AddNode(args *rpcInterfaces.AddNodeArgs, reply *rpcInterfaces.AddNodeReply) error {
	span, closer := args.CreateServerSpan("add-node", node.populateSpan)
	defer closer()
	node.logger.Printf("AddNode request ++")
	reply.Ok = false
	req := Request{
		id:          args.RequestId,
		requestType: AddNode,
		replyAddr:   args.Addr,
		ctx:         span.Context(),
	}

	node.queueRequest(opentracing.ContextWithSpan(context.Background(), span), &req)
	node.logger.Printf("Add node request --")
	reply.Ok = true
	return nil

}

func (node *ChainReplicationNode) handleUpdateRequest(ctx context.Context, updateRequest *Request) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "process-update-request")
	defer span.Finish()

	if node.config.isTail() {
		callback := rpcInterfaces.ResponseCallback{
			Key:       updateRequest.key,
			Value:     updateRequest.value,
			RequestBase : utils.NewRequestBase(),
		}

		callback.RequestId = updateRequest.id
		node.persistRecord(spanCtx, updateRequest)
		node.sendReply(spanCtx, updateRequest.replyAddr, callback)
	} else {
		node.forward(spanCtx, updateRequest)
	}
}
