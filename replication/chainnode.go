package replication

import (
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"net"
	"net/rpc"
	"sync"
)

type ChainNode struct {
	addr                  string
	pending               map[string]Request
	sent                  map[string]Request
	stableStore           *storage.KeyValueStore
	master                string
	lock                  *sync.RWMutex
	logger                utils.Logger
	config                *ChainConfig
	requestProcessorQueue chan Request
	listener              net.Listener
	alive                 bool
	close                 chan interface{}
}

type ChainConfig struct {
	isHead      bool
	isTail      bool
	next        string
	prev        string
	isActive    bool
	queryConfig QueryConfig
}

const (
	Query      = "Query"
	Update     = "Update"
	SyncUpdate = "SyncUpdate"
	SyncAck    = "SyncAck"
)

type Request struct {
	id          string
	requestType string
	key         string
	value       string
	replyAddr   string
}

type ResponseCallback struct {
	Key       string
	Value     string
	RequestId string
	Error     string
}

type NoopReply struct {
}

// Configuration to be considered while servicing query requests
type QueryConfig struct {
	// if true then non-tail Nodes can reply to query requests.
	allowNonTailQuery bool
}

type UpdateConfigArgs struct {
	BecomeHead bool
	BecomeTail bool
	Next       string
	Previous   string
}

type QueryArgs struct {
	Key          string
	RequestId    string
	ReplyAddress string
}

type QueryReply struct {
	Ok bool
}

type UpdateArgs struct {
	Key          string
	Value        string
	RequestId    string
	ReplyAddress string
}

type UpdateReply struct {
	Ok bool
}

func NewChainNode(addr string, db *storage.KeyValueStore, master string, logger utils.Logger) ChainNode {
	chain := ChainNode{
		addr:                  addr,
		pending:               map[string]Request{},
		sent:                  map[string]Request{},
		stableStore:           db,
		master:                master,
		lock:                  &sync.RWMutex{},
		logger:                logger,
		config:                &ChainConfig{},
		requestProcessorQueue: make(chan Request, 1000),
		alive:                 true,
		close:                 make(chan interface{}),
	}

	return chain
}

func (node *ChainNode) OnClose() <- chan interface{}{
	return node.close
}

func (node *ChainNode) Start() {
	rpcs := rpc.NewServer()
	rpcs.Register(node)
	l, err := net.Listen("tcp", node.addr)
	if err != nil {
		node.logger.Fatal().Err(err).Msg("Failed to create callback channel")
		return
	}

	node.listener = l
	go func() {
		args := NodeRegistrationArgs{Address: node.addr}
		var reply NodeRegistrationReply
		node.logger.Info().Msgf("Registering with master %v", node.master)
		utils.Call(node.master, "Master.Register", &args, &reply)
		node.logger.Info().Msgf("Registration done. Reply : %v", reply)
		node.updateConfig(reply)

		for node.alive {
			conn, err := node.listener.Accept()

			if node.alive && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if node.alive == false && err != nil {
				node.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()

	go func() {
		node.processRequest()
	}()
}

func (node *ChainNode) Shutdown() {
	node.alive = false
	node.stableStore.Close()
	node.listener.Close()
	close(node.close)
}

func (node *ChainNode) UpdateConfig() {

}

func (node *ChainNode) canServeQuery() bool {
	defer node.lock.RUnlock()
	node.lock.RLock()
	return node.config.isTail || node.config.queryConfig.allowNonTailQuery
}

func (node *ChainNode) canServeRequest() bool {
	defer node.lock.RUnlock()
	node.lock.RLock()
	return node.config.isActive
}

func (node *ChainNode) canServeUpdate() bool {
	defer node.lock.RUnlock()
	node.lock.RLock()
	return node.config.isHead
}

func (node *ChainNode) Query(request *QueryArgs, reply *QueryReply) error {
	reply.Ok = false
	if !node.canServeRequest() {
		node.lock.RUnlock()
		return errors.NotSupportedf("Unable to service request currently")
	}

	if !node.canServeQuery() {
		return errors.BadRequestf("Cannot service query request")
	}

	req := Request{
		id:          request.RequestId,
		requestType: Query,
		key:         request.Key,
		value:       "",
		replyAddr:   request.ReplyAddress,
	}

	node.handleRequest(req)

	reply.Ok = true
	return nil
}

func (node *ChainNode) Update(request *UpdateArgs, reply *UpdateReply) error {
	reply.Ok = false
	if !node.canServeRequest() {
		return errors.NotSupportedf("Unable to service request currently")
	}

	if !node.canServeUpdate() {
		return errors.BadRequestf("Cannot service update request")
	}

	req := Request{
		id:          request.RequestId,
		requestType: Update,
		key:         request.Key,
		value:       request.Value,
		replyAddr:   request.ReplyAddress,
	}

	node.handleRequest(req)

	reply.Ok = true
	return nil
}

func (node *ChainNode) SyncUpdate(request *UpdateArgs, reply interface{}) error {
	req := Request{
		id:          request.RequestId,
		requestType: SyncUpdate,
		key:         request.Key,
		value:       request.Value,
		replyAddr:   request.ReplyAddress,
	}

	node.handleRequest(req)
	return nil
}

func (node *ChainNode) SyncAck(request *UpdateArgs, reply interface{}) error {
	req := Request{
		id:          request.RequestId,
		requestType: SyncAck,
	}

	node.handleRequest(req)
	return nil
}

func (node *ChainNode) handleRequest(req Request) {
	defer node.lock.Unlock()
	node.lock.Lock()
	_, ok := node.pending[req.id]
	if ok {
		node.logger.Info().Msgf("Ignoring request %v, already in pending state", req)
	} else {
		node.pending[req.id] = req
		// put request in queue and return reply will be sent once request is processed
		node.requestProcessorQueue <- req
	}
}

func (node *ChainNode) processRequest() {
	for req := range node.requestProcessorQueue {
		// tail
		if req.requestType == Query {
			node.logger.Info().Msg("Processing query request")
			record, err := node.stableStore.Get(req.key)
			callback := ResponseCallback{
				Key:       record.Key,
				Value:     "",
				RequestId: req.id,
			}

			if err != nil {
				if errors.IsNotFound(err) {
					callback.Error = "Key not found"
				} else {
					node.logger.Error().Err(err).Msgf("Error while processing query : %v", err)
					callback.Error = "Unexpected error occurred"
				}
			} else {
				callback.Value = record.Value
			}

			node.sendReply(req.replyAddr, callback)
			node.lock.Lock()
			delete(node.pending, req.id)
			node.lock.Unlock()
		} else if req.requestType == Update {
			// Is head and tail both
			if node.config.isTail {
				node.persistRecord(req)
			} else {
				node.forward(req)
				node.lock.Lock()
				node.sent[req.id] = req
				node.lock.Unlock()
			}

		} else if req.requestType == SyncUpdate {
			if node.config.isTail {
				node.persistRecord(req)
			} else {
				node.forward(req)
			}
		} else if req.requestType == SyncAck {
			node.lock.RLock()
			reqAcked, ok := node.pending[req.id]
			node.lock.RUnlock()
			if ok {
				node.stableStore.Write(storage.Record{
					Key:   reqAcked.key,
					Value: reqAcked.value,
				})
				node.lock.Lock()
				delete(node.pending, req.id)
				delete(node.sent, req.id)
				node.lock.Unlock()
				node.sendUpdateAck(req)
			}
		}
	}
}

func (node *ChainNode) persistRecord(req Request) {
	node.stableStore.Write(storage.Record{
		Key:   req.key,
		Value: req.value,
	})

	callback := ResponseCallback{
		Key:       req.key,
		Value:     req.value,
		RequestId: req.id,
	}

	node.sendUpdateAck(req)
	node.sendReply(req.replyAddr, callback)
}

func (node *ChainNode) sendUpdateAck(request Request) {
	if node.config.prev == "" {
		return
	}

	var reply interface{}

	update := UpdateArgs{
		RequestId: request.id,
	}

	// TODO: time out implementation
	err := utils.Call(node.config.prev, "ChainNode.SyncAck", &update, reply)
	if err != nil {
		node.logger.Error().Msgf("Ack %s, failed for requestId: %v", node.config.next, request.id)
	}
}

func (node *ChainNode) sendReply(replyAddr string, callback ResponseCallback) {
	node.logger.Info().Msgf("Sending reply back to client %s, CallbackResponse %v", replyAddr, callback)
	var reply NoopReply
	// TODO: time out implementation
	err := utils.Call(replyAddr, "ServiceClient.Callback", &callback, &reply)
	if err != nil {
		node.logger.Error().Err(err).Msgf("Callback %s, failed for requestId: %v", replyAddr, callback.RequestId)
	} else {
		node.logger.Info().Msgf("Reply sent to %s for %s", replyAddr, callback.RequestId)
	}

}

func (node *ChainNode) forward(request Request) {
	if node.config.next == "" {
		return
	}

	var reply interface{}
	update := UpdateArgs{
		Key:          request.key,
		Value:        request.value,
		RequestId:    request.id,
		ReplyAddress: request.replyAddr,
	}

	// TODO: time out implementation
	err := utils.Call(node.config.next, "ChainNode.SyncUpdate", &update, reply)
	if err != nil {
		node.logger.Error().Msgf("Sync %s, failed for requestId: %v", node.config.next, request.id)
	}
}

func (node *ChainNode) updateConfig(reply NodeRegistrationReply) {
	defer node.lock.Unlock()
	node.lock.Lock()
	node.config.isTail = reply.IsTail
	node.config.isHead = reply.IsHead
	node.config.next = reply.Next
	node.config.prev = reply.Previous
	node.config.isActive = true
}
