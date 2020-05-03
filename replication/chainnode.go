package replication

import (
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"net"
	"net/rpc"
	"sync"
	"time"
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

type HeartBeatArgs struct {
	Sender string
}

// Configuration to be considered while servicing query requests
type QueryConfig struct {
	// if true then non-tail Nodes can reply to query requests.
	allowNonTailQuery bool
}

type ChainLinkUpdate struct {
	Next       string
	Prev       string
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

	go node.processRequest()
	go node.sendHeartBeat()
}

func (node *ChainNode) Shutdown() {
	node.alive = false
	node.stableStore.Close()
	node.listener.Close()
	close(node.close)
}

func (node *ChainNode) UpdateTail(args *ChainLinkUpdate, reply *NoopReply) error {
	node.logger.Info().Msgf("Received update config command for %s", node.addr)
	defer node.lock.Unlock()
	node.lock.Lock()
	node.config.next = args.Next
	node.config.prev = args.Prev
	node.config.isTail = node.config.next == ""
	node.config.isHead = node.config.prev == ""
	node.logger.Info().Msgf("Updated config for %, IsTail: %s, IsHead: %s, Next : %s, Prev : %s", node.addr, node.config.isTail, node.config.isHead, node.config.next, node.config.prev)
	return nil
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
	node.logger.Printf("Query request ++")
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
	node.logger.Printf("Query request queued")
	node.logger.Printf("Query request --")
	reply.Ok = true
	return nil
}

func (node *ChainNode) Update(request *UpdateArgs, reply *UpdateReply) error {
	node.logger.Printf("Update request ++")
	reply.Ok = false
	if !node.canServeRequest() {
		node.logger.Printf("Unable to service request currently")
		return errors.NotSupportedf("Unable to service request currently")
	}

	if !node.canServeUpdate() {
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

	node.handleRequest(req)
	node.logger.Printf("Update request --")
	reply.Ok = true
	return nil
}

func (node *ChainNode) SyncUpdate(request *UpdateArgs, reply *NoopReply) error {
	node.logger.Printf("SyncUpdate request ++")
	req := Request{
		id:          request.RequestId,
		requestType: SyncUpdate,
		key:         request.Key,
		value:       request.Value,
		replyAddr:   request.ReplyAddress,
	}

	node.handleRequest(req)
	node.logger.Printf("SyncUpdate request queued")
	node.logger.Printf("SyncUpdate request --")
	return nil
}

func (node *ChainNode) SyncAck(request *UpdateArgs, reply *NoopReply) error {
	node.logger.Printf("SyncAck request queued")
	req := Request{
		id:          request.RequestId,
		requestType: SyncAck,
	}

	node.handleRequest(req)
	node.logger.Printf("SyncAck request queued")
	node.logger.Printf("SyncAck request --")
	return nil
}

func (node *ChainNode) handleRequest(req Request) {
	node.logger.Printf("Handle request %s ++", req.requestType)
	defer node.lock.Unlock()
	node.lock.Lock()
	_, ok := node.pending[req.id]
	if ok {
		node.logger.Info().Msgf("Ignoring request %v, already in pending state", req)
	} else {
		node.logger.Printf("Handle request %s, id: %s add to pending state", req.requestType, req.id)
		node.pending[req.id] = req
		// put request in queue and return reply will be sent once request is processed
		node.requestProcessorQueue <- req
		node.logger.Printf("Handle request %s, id: %s added to queue", req.requestType, req.id)
	}
	node.logger.Printf("Handle request %s --", req.requestType)
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

	var reply NoopReply

	update := UpdateArgs{
		RequestId: request.id,
	}

	// TODO: time out implementation
	err := utils.Call(node.config.prev, "ChainNode.SyncAck", &update, &reply)
	if err != nil {
		node.logger.Error().Msgf("Ack %s, failed for requestId: %v", node.config.next, request.id)
	}
}

func (node *ChainNode) sendReply(replyAddr string, callback ResponseCallback) {
	node.logger.Info().Msgf("Sending reply back from %s to client %s, CallbackResponse %v", node.addr, replyAddr, callback)
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

	var reply NoopReply
	update := UpdateArgs{
		Key:          request.key,
		Value:        request.value,
		RequestId:    request.id,
		ReplyAddress: request.replyAddr,
	}

	// TODO: time out implementation
	err := utils.Call(node.config.next, "ChainNode.SyncUpdate", &update, &reply)
	if err != nil {
		node.logger.Error().Err(err).Msgf("Sync %s, failed for requestId: %v", node.config.next, request.id)
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

	node.logger.Info().Msgf("Updated config for %, IsTail: %s, IsHead: %s, Next : %s, Prev : %s", node.addr, node.config.isTail, node.config.isHead, node.config.next, node.config.prev)
}

func (node *ChainNode) sendHeartBeat() {
	hbTimer := time.NewTicker(time.Second * 1)
	req := HeartBeatArgs{ Sender: node.addr }

	for t := range hbTimer.C {
		if !node.alive {
			break
		}

		node.logger.Printf("Sending heartbeat at %v", t)
		var reply NoopReply
		err := utils.Call(node.master, "Master.Heartbeat", &req, &reply)
		if err != nil {
			node.logger.Error().Err(err).Msg("Failed to send heart beat signal to master")
		}

	}
}
