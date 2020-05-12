package node

import (
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type ChainReplicationNode struct {
	addr                  string
	pending               *utils.Chain
	sent                  *utils.Chain
	stableStore           *storage.KeyValueStore
	master                string
	lock                  *sync.RWMutex
	logger                utils.Logger
	config                *ChainConfig
	requestProcessorQueue chan *Request
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
	lock        sync.RWMutex
}

func NewChainConfig() *ChainConfig {
	return &ChainConfig{
		isHead:      false,
		isTail:      false,
		next:        "",
		prev:        "",
		isActive:    false,
		queryConfig: QueryConfig{},
		lock:        sync.RWMutex{},
	}
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

// Configuration to be considered while servicing query requests
type QueryConfig struct {
	// if true then non-tail Nodes can reply to query requests.
	allowNonTailQuery bool
}

func NewChainReplicationNode(addr string, db *storage.KeyValueStore, master string, logger utils.Logger) ChainReplicationNode {
	chain := ChainReplicationNode{
		addr:                  addr,
		pending:               utils.NewChain(),
		sent:                  utils.NewChain(),
		stableStore:           db,
		master:                master,
		lock:                  &sync.RWMutex{},
		logger:                logger,
		config:                &ChainConfig{},
		requestProcessorQueue: make(chan *Request, 1000),
		alive:                 true,
		close:                 make(chan interface{}),
	}

	return chain
}

func (node *ChainReplicationNode) OnClose() <-chan interface{} {
	return node.close
}

func (node *ChainReplicationNode) Start() error {
	rpcs := rpc.NewServer()
	rpcs.Register(node)
	l, err := net.Listen("tcp", node.addr)
	if err != nil {
		node.logger.Fatal().Err(err).Msg("Failed to create callback channel")
		return err
	}

	node.listener = l
	go func() {
		args := rpcInterfaces.NodeRegistrationArgs{Address: node.addr}
		var reply rpcInterfaces.NodeRegistrationReply
		node.logger.Info().Msgf("Registering with master %v", node.master)
		utils.Call(node.master, "Master.Register", &args, &reply)
		node.logger.Info().Msgf("Registration done. Reply : %v", reply)
		node.updateConfig(reply)

		for node.alive {
			conn, err := node.listener.Accept()

			if node.alive && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if node.alive && err != nil {
				node.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()

	go node.processRequest()
	go node.sendHeartBeat()
	return nil
}

func (node *ChainReplicationNode) Shutdown() {
	node.alive = false
	node.stableStore.Close()
	node.listener.Close()
	close(node.close)
}

func (node *ChainReplicationNode) CanServeQuery() bool {
	defer node.lock.RUnlock()
	node.lock.RLock()
	return node.canServeQuery()
}

func (node *ChainReplicationNode) CanServeRequest() bool {
	defer node.lock.RUnlock()
	node.lock.RLock()
	return node.canServeRequest()
}

func (node *ChainReplicationNode) CanServeUpdate() bool {
	defer node.lock.RUnlock()
	node.lock.RLock()
	return node.canServeUpdate()
}

func (node *ChainReplicationNode) canServeQuery() bool {
	return node.config.isTail || node.config.queryConfig.allowNonTailQuery
}

func (node *ChainReplicationNode) canServeRequest() bool {
	return node.config.isActive
}

func (node *ChainReplicationNode) canServeUpdate() bool {
	return node.config.isHead
}

func (node *ChainReplicationNode) sendHeartBeat() {
	hbTimer := time.NewTicker(time.Second * 1)
	req := rpcInterfaces.HeartBeatArgs{Sender: node.addr}

	for range hbTimer.C {
		if !node.alive {
			break
		}

		//node.logger.Printf("Sending heartbeat at %v", t)
		var reply rpcInterfaces.NoopReply
		err := utils.Call(node.master, "Master.Heartbeat", &req, &reply)
		if err != nil {
			node.logger.Error().Err(err).Msg("Failed to send heart beat signal to master")
		}

	}
}

func (node *ChainReplicationNode) updateConfig(reply rpcInterfaces.NodeRegistrationReply) {
	defer node.lock.Unlock()
	node.lock.Lock()
	node.config.isTail = reply.IsTail
	node.config.isHead = reply.IsHead
	node.config.next = reply.Next
	node.config.prev = reply.Previous
	node.config.isActive = true

	node.logger.Info().Msgf("Updated config for %, IsTail: %s, IsHead: %s, Next : %s, Prev : %s",
		node.addr,
		node.config.isTail,
		node.config.isHead,
		node.config.next,
		node.config.prev)
}

func (node *ChainReplicationNode) processRequest() {
	for req := range node.requestProcessorQueue {
		node.config.lock.RLock()
		if req.requestType == Query && node.canServeQuery() {
			node.handleQueryRequest(req)
		} else if req.requestType == Update && node.canServeUpdate() {
			node.handleUpdateRequest(req)
		} else if req.requestType == SyncAck {
			node.handleSyncUpdateAck(req)
		} else if req.requestType == SyncUpdate {
			node.handleUpdateRequest(req)
		}

		node.config.lock.RUnlock()
	}
}

func (node *ChainReplicationNode) queueRequest(req *Request) {
	node.logger.Printf("Handle request %s ++", req.requestType)
	chainRequest := utils.NewChainLink(req, req.id)
	defer node.lock.Unlock()
	node.lock.Lock()
	var err error
	// this is kind ack/reply from successor node, just queue the request without any pending/sent updates
	if req.requestType == SyncAck {
		err = nil
	} else {
		_, err = node.pending.Add(chainRequest)
		node.logger.Printf("Handle request %s, id: %s added to pending state", req.requestType, req.id)
	}

	if errors.IsAlreadyExists(err) {
		node.logger.Info().Msgf("Ignoring request %v, already in pending state", req)
	} else if err != nil {
		node.logger.Error().Err(err).Msgf("Request with id %s, ignored because of unexpected error", req.id)
	} else {
		// put request in queue and return reply will be sent once request is processed
		node.requestProcessorQueue <- req
		node.logger.Printf("Handle request %s, id: %s added to queue", req.requestType, req.id)
	}
	node.logger.Printf("Handle request %s --", req.requestType)
}

func (node *ChainReplicationNode) persistRecord(req *Request) {
	_, err := node.pending.RemoveById(req.id)

	if errors.IsNotFound(err) {
		node.logger.Info().Msgf("Request with id %s, is not in pending state for %s", req.id, node.addr)
		return
	}

	node.stableStore.Write(storage.Record{
		Key:   req.key,
		Value: req.value,
	})
	node.sendUpdateAck(req)
}

func (node *ChainReplicationNode) sendUpdateAck(request *Request) {
	if node.config.prev == "" {
		return
	}

	var reply rpcInterfaces.NoopReply

	update := rpcInterfaces.UpdateArgs{
		RequestId: request.id,
	}

	// TODO: time out implementation
	err := utils.Call(node.config.prev, "ChainReplicationNode.SyncAck", &update, &reply)
	if err != nil {
		node.logger.Error().Msgf("Ack %s, failed for requestId: %v", node.config.next, request.id)
	}
}

func (node *ChainReplicationNode) forward(request *Request) {
	if node.config.next == "" {
		return
	}

	var reply rpcInterfaces.NoopReply
	update := rpcInterfaces.UpdateArgs{
		Key:          request.key,
		Value:        request.value,
		RequestId:    request.id,
		ReplyAddress: request.replyAddr,
	}

	// making an assumption if sent results in failure then successor will eventually be removed from chain by failure detector
	// adding it sent history then makes this case similar to the case where req reached succ then succ node died.
	node.sent.Add(utils.NewChainLink(request, request.id))
	// TODO: time out implementation
	err := utils.Call(node.config.next, "ChainReplicationNode.SyncUpdate", &update, &reply)
	if err != nil {
		node.logger.Error().Err(err).Msgf("Sync update from %s to %s, failed for requestId: %v", node.addr, node.config.next, request.id)
	}
}

// handle sync acknowledgement
func (node *ChainReplicationNode) handleSyncUpdateAck(r *Request) {
	node.sent.RemoveById(r.id)
	node.persistRecord(r)
}

func (node *ChainReplicationNode) handleSyncUpdate(r *Request) {
	// end of chain, persist and send ack back
	if node.config.isTail {
		node.persistRecord(r)
	} else {
		// keep forwarding the request until it reaches tail
		node.forward(r)
	}
}

func (node *ChainReplicationNode) getLastRequestIdReceivedBySuccessor() string {

	if node.config.next == "" {
		return ""
	}

	req := rpcInterfaces.LastRequestReceivedRequest{}
	var rep rpcInterfaces.LastRequestReceivedReply
	node.logger.Printf("Fetching last request received by %s", node.config.next)
	err := utils.Call(node.config.next, "ChainReplicationNode.LastRequestId", &req, &rep)
	if err != nil {
		node.logger.Error().Err(err).Msgf("Failed to fetch last request received from: %v", node.config.next)
		return ""
	}
	node.logger.Printf("Fetch complete last request received by %s, Request Id : %s", node.config.next, rep.Id)
	return rep.Id
}

func (node *ChainReplicationNode) replaySentRequest(startAtRequestId string) {
	var requestsToBeResend []*Request
	// if startAt is empty then resend all requests
	found := startAtRequestId == ""
	node.sent.IterFunc(func(r *utils.ChainLink) {
		request := r.Data().(*Request)
		if found {
			requestsToBeResend = append(requestsToBeResend, request)
		} else {
			found = request.id == startAtRequestId
		}
	})

	for _, r := range requestsToBeResend {
		node.logger.Info().Msgf("Resending %s, from %s to %s", r.id, node.addr, node.config.next)
		node.forward(r)
	}
}