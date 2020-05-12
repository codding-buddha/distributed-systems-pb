package node

import (
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
)

func (node *ChainReplicationNode) SyncUpdate(request *rpcInterfaces.UpdateArgs, reply *rpcInterfaces.NoopReply) error {
	node.logger.Printf("SyncUpdate request ++")
	req := Request{
		id:          request.RequestId,
		requestType: SyncUpdate,
		key:         request.Key,
		value:       request.Value,
		replyAddr:   request.ReplyAddress,
	}

	node.queueRequest(&req)
	node.logger.Printf("SyncUpdate request queued")
	node.logger.Printf("SyncUpdate request --")
	return nil
}

func (node *ChainReplicationNode) SyncAck(request *rpcInterfaces.UpdateArgs, reply *rpcInterfaces.NoopReply) error {
	node.logger.Printf("SyncAck request queued")
	c, ok := node.pending.GetById(request.RequestId)
	if !ok {
		node.logger.Error().Msgf("Ignoring Ack for non-existent request id %s", request.RequestId)
		return errors.NotFoundf("Request with id %s, not present pending state of %s", request.RequestId, node.addr)
	}

	r := c.Data().(*Request)

	req := Request{
		id:          request.RequestId,
		requestType: SyncAck,
		value: r.value,
		replyAddr: r.replyAddr,
		key: r.key,
	}

	node.queueRequest(&req)
	node.logger.Printf("SyncAck request queued")
	node.logger.Printf("SyncAck request --")
	return nil
}

func (node *ChainReplicationNode) SyncConfig(request *rpcInterfaces.SyncConfigurationRequest, reply *rpcInterfaces.SyncConfigurationReply) error {
	defer node.config.lock.Unlock()
	node.config.lock.Lock()
	node.logger.Printf("Config before update : ")
	node.logConfig()

	node.syncConfig(request)
	if node.config.isTail {
		node.logger.Info().Msgf("Handle chain reconfiguration, node %s isTail", node.addr)
		node.handleConfigurationUpdateForTail()
	} else {
		// replay requests to new successor to ensure that failure in middle of the chain does not result in some requests being lost
		lastReceivedRequestIdBySuccessor := node.getLastRequestIdReceivedBySuccessor()
		node.replaySentRequest(lastReceivedRequestIdBySuccessor)
	}

	node.logger.Printf("Config after reconfiguration : ")
	node.logConfig()
	node.logger.Printf("Chain reconfiguration done for node %s", node.addr)
	reply.Ok = true
	return nil
}

func (node *ChainReplicationNode) LastRequestId(request *rpcInterfaces.LastRequestReceivedRequest, reply *rpcInterfaces.LastRequestReceivedReply) error {
	reply.Id = ""
	e := node.pending.Tail()
	var startAt *utils.ChainLink
	for ; !e.IsNull() ; e = e.Previous() {
		if getRequest(e).requestType == Update {
			startAt = e
			break
		}
	}

	if startAt != nil && !startAt.IsNull() {
		reply.Id = getRequest(startAt).id
	}

	return nil
}

func getRequest(c *utils.ChainLink) *Request {
	if c.IsNull() {
		return nil
	}
	return c.Data().(*Request)
}

func (node *ChainReplicationNode) syncConfig(newConfig *rpcInterfaces.SyncConfigurationRequest) {
	node.config.next = newConfig.Next
	node.config.prev = newConfig.Previous
	node.config.isTail = node.config.next == ""
	node.config.isHead = node.config.prev == ""
}

func (node *ChainReplicationNode) logConfig() {
	node.logger.Printf("Node configuration for node %s, isHead: %s, isTail: %s, Prev: %s, Next: %s",
		node.addr,
		node.config.isHead,
		node.config.isTail,
		node.config.prev,
		node.config.next)
}