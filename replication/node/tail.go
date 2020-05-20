package node

import (
	"context"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
)
import rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"

func (node *ChainReplicationNode) Query(request *rpcInterfaces.QueryArgs, reply *rpcInterfaces.QueryReply) error {
	span, closer := request.CreateServerSpan("query")
	defer closer()
	node.populateSpan(span)
	node.logger.Printf("Query request ++")
	reply.Ok = false
	if !node.CanServeRequest() {
		err := errors.NotSupportedf("Unable to service request currently")
		ext.Error.Set(span, true)
		span.LogFields(log.String("event", "not in ready state"), log.Error(err))
		return err
	}

	if !node.CanServeQuery() {
		err := errors.BadRequestf("Cannot service query request")
		ext.Error.Set(span, true)
		span.LogFields(log.String("event", "not allowed to serve query"), log.Error(err))
		return err
	}

	req := Request{
		id:          request.RequestId,
		requestType: Query,
		key:         request.Key,
		value:       "",
		replyAddr:   request.ReplyAddress,
	}

	err := node.handleQueryRequest(opentracing.ContextWithSpan(context.Background(), span), &req, reply)
	node.logger.Printf("Query request --")
	if err != nil {
		node.logger.Printf("Query failed with error : %v", err)
		node.logger.Error().Err(err)
		ext.Error.Set(span, true)
		span.LogFields(log.String("event", "query failure"), log.Error(err))
		return err
	}

	reply.Ok = true
	return nil
}

func (node *ChainReplicationNode) handleQueryRequest(ctx context.Context, req *Request, rep *rpcInterfaces.QueryReply) error {
	node.logger.Info().Msg("Processing query request")
	record, err := node.stableStore.Get(ctx, req.key)
	rep.Key = req.key

	if err != nil {
		if !errors.IsNotFound(err) {
			node.logger.Error().Err(err).Msgf("Error while processing query : %v", err)
		}

		return err
	}

	rep.Value = record.Value
	return nil
}

func (node *ChainReplicationNode) sendReply(ctx context.Context, replyAddr string, callback rpcInterfaces.ResponseCallback) {
	node.logger.Info().Msgf("Sending reply back from %s to client %s, CallbackResponse %v", node.addr, replyAddr, callback)
	var reply rpcInterfaces.NoopReply
	// TODO: time out implementation
	err := utils.TraceableCall(replyAddr, "ServiceClient.Callback", &callback, &reply, ctx)
	if err != nil {
		node.logger.Error().Err(err).Msgf("Callback %s, failed for requestId: %v", replyAddr, callback.RequestId)
	} else {
		node.logger.Info().Msgf("Reply sent to %s for %s", replyAddr, callback.RequestId)
	}
}

func (node *ChainReplicationNode) sendErrorReply(ctx context.Context, replyAddr string, error string, id string) {
	errorResponse := rpcInterfaces.ResponseCallback{
		Key:       "",
		Value:     "",
		RequestBase : utils.NewRequestBase(),
		Error:     error,
	}

	errorResponse.RequestId = id
	node.sendReply(ctx, replyAddr, errorResponse)
}

func (node *ChainReplicationNode) handleConfigurationUpdateForTail(ctx context.Context) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "commit-speculative-history")
	defer span.Finish()
	// node is now tail because of node failures in chain, commit all sent requests
	var uncommittedRequests []*Request
	node.sent.IterFunc(func(request *utils.ChainLink) {
		r := request.Data().(*Request)
		uncommittedRequests = append(uncommittedRequests, r)
	})

	node.sent.Clear()

	for _, req := range uncommittedRequests {
		if req.requestType == AddNode {
			node.handleAddNode(spanCtx, req)
		} else {
			node.handleUpdateRequest(spanCtx, req)
		}
	}
}

func (node *ChainReplicationNode) sendHistory(ctx context.Context, request *Request) (bool, error) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "sync-history")
	defer  span.Finish()
	if node.addr == request.replyAddr {
		return false, nil
	}

	sendTo := request.replyAddr
	node.logger.Info().Msgf("Sync history from %s to %s", node.addr, sendTo)
	records, err := node.stableStore.GetAll(spanCtx)
	var reply rpcInterfaces.SyncHistoryReply
	history := rpcInterfaces.SyncHistoryArgs{
		History: records,
		RequestBase: utils.NewRequestBase(),
	}
	history.RequestId = request.id
	// TODO: time out implementation
	err = utils.TraceableCall(sendTo, "ChainReplicationNode.SyncHistory", &history, &reply, spanCtx)
	if err != nil {
		ext.Error.Set(span, true)
		span.LogFields(log.String("event", "history sync failure"), log.Error(err))
		node.logger.Error().Err(err).Msgf("Sync history failed from %s to %s", node.addr, request.replyAddr)
		return false, err
	} else {
		node.logger.Info().Msgf("Sync successful  from %s to %s", node.addr, request.replyAddr)
	}

	return true, nil
}
