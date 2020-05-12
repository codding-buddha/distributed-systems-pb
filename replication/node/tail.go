package node

import (
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
)
import rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"

func (node *ChainReplicationNode) Query(request *rpcInterfaces.QueryArgs, reply *rpcInterfaces.QueryReply) error {
	node.logger.Printf("Query request ++")
	reply.Ok = false
	if !node.CanServeRequest() {
		return errors.NotSupportedf("Unable to service request currently")
	}

	if !node.CanServeQuery() {
		return errors.BadRequestf("Cannot service query request")
	}

	req := Request{
		id:          request.RequestId,
		requestType: Query,
		key:         request.Key,
		value:       "",
		replyAddr:   request.ReplyAddress,
	}

	node.queueRequest(&req)
	node.logger.Printf("Query request queued")
	node.logger.Printf("Query request --")
	reply.Ok = true
	return nil
}

func (node *ChainReplicationNode) handleQueryRequest(req *Request) {
	node.logger.Info().Msg("Processing query request")
	record, err := node.stableStore.Get(req.key)
	callback := rpcInterfaces.ResponseCallback{
		Key:       record.Key,
		Value:     "",
		RequestId: req.id,
	}

	if err != nil {
		if errors.IsNotFound(err) {
			callback.Error = "Key '" + req.key + "' not found"
		} else {
			node.logger.Error().Err(err).Msgf("Error while processing query : %v", err)
			callback.Error = "Unexpected error occurred"
		}
	} else {
		callback.Value = record.Value
	}

	node.sendReply(req.replyAddr, callback)
	node.pending.RemoveById(req.id)
}

func (node *ChainReplicationNode) sendReply(replyAddr string, callback rpcInterfaces.ResponseCallback) {
	node.logger.Info().Msgf("Sending reply back from %s to client %s, CallbackResponse %v", node.addr, replyAddr, callback)
	var reply rpcInterfaces.NoopReply
	// TODO: time out implementation
	err := utils.Call(replyAddr, "ServiceClient.Callback", &callback, &reply)
	if err != nil {
		node.logger.Error().Err(err).Msgf("Callback %s, failed for requestId: %v", replyAddr, callback.RequestId)
	} else {
		node.logger.Info().Msgf("Reply sent to %s for %s", replyAddr, callback.RequestId)
	}
}

func (node *ChainReplicationNode) handleConfigurationUpdateForTail() {
	// node is now tail because of node failures in chain, commit all sent requests
	var uncommittedRequests []*Request
	node.sent.IterFunc(func (request *utils.ChainLink) {
		r := request.Data().(*Request)
		uncommittedRequests = append(uncommittedRequests, r)
	})

	node.sent.Clear()

	for _, req := range uncommittedRequests {
		node.handleUpdateRequest(req)
	}
}