package node

import (
	"context"
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
)

func (node *ChainReplicationNode) SyncUpdate(request *rpcInterfaces.UpdateArgs, reply *rpcInterfaces.NoopReply) error {
	span, closer := request.CreateServerSpan("chain-update", node.populateSpan)
	defer closer()
	node.logger.Printf("SyncUpdate request ++")
	req := Request{
		id:          request.RequestId,
		requestType: SyncUpdate,
		key:         request.Key,
		value:       request.Value,
		replyAddr:   request.ReplyAddress,
		ctx:         span.Context(),
	}

	node.queueRequest(opentracing.ContextWithSpan(context.Background(), span), &req)
	node.logger.Printf("SyncUpdate request queued")
	node.logger.Printf("SyncUpdate request --")
	return nil
}

func (node *ChainReplicationNode) SyncHistory(request *rpcInterfaces.SyncHistoryArgs, reply *rpcInterfaces.SyncHistoryReply) error {
	span, closer := request.CreateServerSpan("sync-history", node.populateSpan)
	defer closer()
	node.logger.Printf("SyncHistory ++")
	reply.Ok = false
	defer node.config.lock.Unlock()
	node.config.lock.Lock()
	if node.config.isActive {
		node.logger.Warn().Msgf("Received sync history request for already active node %s, rejecting request", node.addr)
		return nil
	}

	ctx := opentracing.ContextWithSpan(context.Background(), span)

	err := node.stableStore.BulkUpdate(ctx, &request.History)

	if err != nil {
		return err
	}

	// notify master that node is ready
	err = node.completeRegistration(ctx, request.RequestId)
	if err != nil {
		return err
	}

	reply.Ok = true
	node.logger.Printf("SyncHistory --")
	return nil
}

func (node *ChainReplicationNode) SyncAck(request *rpcInterfaces.UpdateArgs, reply *rpcInterfaces.NoopReply) error {
	span, closer := request.CreateServerSpan("sync-ack", node.populateSpan)
	defer closer()
	node.logger.Printf("SyncAck request ++")
	c, ok := node.pending.GetById(request.RequestId)
	if !ok {
		node.logger.Error().Msgf("Ignoring Ack for non-existent request id %s", request.RequestId)
		return errors.NotFoundf("Request with id %s, not present pending state of %s", request.RequestId, node.addr)
	}

	r := c.Data().(*Request)

	req := Request{
		id:          request.RequestId,
		requestType: SyncAck,
		value:       r.value,
		replyAddr:   r.replyAddr,
		key:         r.key,
		ctx:         span.Context(),
	}

	node.queueRequest(opentracing.ContextWithSpan(context.Background(), span), &req)
	node.logger.Printf("SyncAck request queued")
	node.logger.Printf("SyncAck request --")
	return nil
}

func (node *ChainReplicationNode) SyncConfig(request *rpcInterfaces.SyncConfigurationRequest, reply *rpcInterfaces.SyncConfigurationReply) error {
	span, closer := request.CreateServerSpan("sync-chain-links", node.populateSpan)
	defer closer()

	defer node.config.lock.Unlock()
	node.config.lock.Lock()

	node.logger.Printf("Config before update : ")
	node.logConfig()

	node.syncConfig(request)
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	if node.config.isTail() {
		node.logger.Info().Msgf("Handle chain reconfiguration, node %s isTail", node.addr)
		node.handleConfigurationUpdateForTail(ctx)
	} else {
		if request.FirstPendingRequestId != "nil" || request.LastPendingRequestId != "nil" {
			// replay requests to new successor to ensure that failure in middle of the chain does not result in some requests being lost
			node.replaySentRequest(ctx, request.FirstPendingRequestId, request.LastPendingRequestId)
		}
	}

	node.logger.Printf("Config after reconfiguration : ")
	node.logConfig()
	node.logger.Printf("Chain reconfiguration done for node %s", node.addr)
	if node.pending.IsEmpty() {
		reply.FirstPendingRequestId = ""
		reply.LastPendingRequestId = ""
	} else {
		reply.FirstPendingRequestId = node.pending.Head().Id()
		reply.LastPendingRequestId = node.pending.Tail().Id()
	}

	reply.Ok = true
	return nil
}

func (node *ChainReplicationNode) SyncAddNode(request *rpcInterfaces.AddNodeArgs, reply *rpcInterfaces.NoopReply) error {
	defer node.config.lock.Unlock()
	node.config.lock.Lock()

	span, closer := request.CreateServerSpan("add-node-ack", node.populateSpan)
	defer closer()

	node.logger.Printf("AddNodeAck ++")
	c, ok := node.pending.GetById(request.RequestId)
	if !ok {
		node.logger.Error().Msgf("Ignoring Ack for non-existent request id %s", request.RequestId)
		return errors.NotFoundf("Request with id %s, not present pending state of %s", request.RequestId, node.addr)
	}

	r := c.Data().(*Request)

	req := Request{
		id:          request.RequestId,
		requestType: AddNodeAck,
		replyAddr:   r.replyAddr,
		ctx:         span.Context(),
	}

	node.queueRequest(opentracing.ContextWithSpan(context.Background(), span), &req)
	node.logger.Printf("AddNodeAck request queued")
	node.logger.Printf("AddNodeAck request --")
	return nil

	return nil
}

func (node *ChainReplicationNode) syncConfig(newConfig *rpcInterfaces.SyncConfigurationRequest) {
	node.config.next = newConfig.Next
	node.config.prev = newConfig.Previous
}

func (node *ChainReplicationNode) logConfig() {
	node.logger.Printf("Node configuration for node %s, isHead: %s, isTail: %s, Prev: %s, Next: %s",
		node.addr,
		node.config.isHead(),
		node.config.isTail(),
		node.config.prev,
		node.config.next)
}

func (node *ChainReplicationNode) completeRegistration(ctx context.Context, reqId string) error {
	args := rpcInterfaces.NodeReadyArgs{
		Addr:        node.addr,
		RequestBase: utils.NewRequestBase(),
	}

	args.RequestId = reqId
	var reply *rpcInterfaces.NodeRegistrationReply
	node.logger.Printf("Notifying master that node is ready")

	error := utils.TraceableCall(node.master, "Master.NodeReady", &args, &reply, ctx)
	if error != nil {
		node.logger.Error().Err(error).Msgf("Failed to complete node registration")
	} else {
		node.updateConfig(ctx, *reply)
		node.logger.Printf("Node %s, added to chain", node.addr)
	}
	return error
}
