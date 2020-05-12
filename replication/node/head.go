package node

import (
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/juju/errors"
)

func (node *ChainReplicationNode) Update(request *rpcInterfaces.UpdateArgs, reply *rpcInterfaces.UpdateReply) error {
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
	}

	node.queueRequest(&req)
	node.logger.Printf("Update request --")
	reply.Ok = true
	return nil
}

func (node *ChainReplicationNode) handleUpdateRequest(updateRequest *Request) {
	// single node
	if node.config.isTail {
		callback := rpcInterfaces.ResponseCallback{
			Key:       updateRequest.key,
			Value:     updateRequest.value,
			RequestId: updateRequest.id,
		}

		node.persistRecord(updateRequest)
		node.sendReply(updateRequest.replyAddr, callback)
	} else {
		node.forward(updateRequest)
	}
}












