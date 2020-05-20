package node

import (
	"context"
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
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
	next        string
	prev        string
	isActive    bool
	queryConfig QueryConfig
	lock        sync.RWMutex
}

func (config *ChainConfig) isTail() bool {
	return config.next == ""
}

func (config *ChainConfig) isHead() bool {
	return config.prev == ""
}

func NewChainConfig() *ChainConfig {
	return &ChainConfig{
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
	AddNode    = "AddNode"
	AddNodeAck = "AddNodeAck"
)

type Request struct {
	id          string
	requestType string
	key         string
	value       string
	replyAddr   string
	ctx         opentracing.SpanContext
}

func (req *Request) isAck() bool {
	return req.requestType == AddNodeAck || req.requestType == SyncAck
}

func (req *Request) isUpdate() bool {
	return req.requestType == Update || req.requestType == AddNode
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
		args := rpcInterfaces.NodeRegistrationStartArgs{Address: node.addr, RequestBase: utils.NewRequestBase()}
		var reply rpcInterfaces.NodeRegistrationReply
		node.logger.Info().Msgf("Registering with master %v", node.master)
		tracer := opentracing.GlobalTracer()
		span := tracer.StartSpan("registration")
		ctx := opentracing.ContextWithSpan(context.Background(), span)
		utils.TraceableCall(node.master, "Master.Register", &args, &reply, ctx)
		node.logger.Info().Msgf("Registration done. Reply : %v", reply)
		node.updateConfig(ctx, reply)
		span.Finish()

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
	return node.config.isTail() || node.config.queryConfig.allowNonTailQuery
}

func (node *ChainReplicationNode) canServeRequest() bool {
	return node.config.isActive
}

func (node *ChainReplicationNode) canServeUpdate() bool {
	return node.config.isHead()
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

func (node *ChainReplicationNode) updateConfig(ctx context.Context, reply rpcInterfaces.NodeRegistrationReply) {
	span, _ := opentracing.StartSpanFromContext(ctx, "update-chain-links")
	defer span.Finish()
	defer node.lock.Unlock()
	node.lock.Lock()
	if reply.AddedToChain {
		node.config.next = reply.Next
		node.config.prev = reply.Previous
		node.config.isActive = true

		node.logger.Info().Msgf("Updated config for %, IsTail: %s, IsHead: %s, Next : %s, Prev : %s",
			node.addr,
			node.config.isTail(),
			node.config.isHead(),
			node.config.next,
			node.config.prev)
		span.LogFields(log.String("event", "node registration complete"))
	} else {
		node.logger.Info().Msgf("Node will be added to chain after history is synced")
		node.config.isActive = false
		span.LogFields(log.String("event", "node registration pending. wait for history sync"))
	}

	span.SetTag("chain.link.prev", reply.Previous)
	span.SetTag("chain.link.next", reply.Next)
}

func (node *ChainReplicationNode) processRequest() {
	for req := range node.requestProcessorQueue {
		spanRef := opentracing.FollowsFrom(req.ctx)
		span := opentracing.StartSpan("process-request", spanRef)
		node.config.lock.RLock()
		ctx := opentracing.ContextWithSpan(context.Background(), span)
		if req.requestType == Update && node.canServeUpdate() {
			node.handleUpdateRequest(ctx, req)
		} else if req.requestType == SyncAck {
			node.handleSyncUpdateAck(ctx, req)
		} else if req.requestType == SyncUpdate {
			node.handleUpdateRequest(ctx, req)
		} else if req.requestType == AddNode {
			node.handleAddNode(ctx, req)
		} else if req.requestType == AddNodeAck {
			node.handleAddNodeAck(ctx, req)
		}
		node.config.lock.RUnlock()
		span.Finish()
	}
}

func (node *ChainReplicationNode) queueRequest(ctx context.Context, req *Request) {
	span, _ := opentracing.StartSpanFromContext(ctx, "queue-request")
	defer span.Finish()
	node.logger.Printf("Handle request %s ++", req.requestType)
	chainRequest := utils.NewChainLink(req, req.id)
	defer node.lock.Unlock()
	node.lock.Lock()
	var err error
	// this is kind ack/reply from successor node, just queue the request without any pending/sent updates
	if req.isAck() {
		err = nil
	} else {
		_, err = node.pending.Add(chainRequest)
		node.logger.Printf("Handle request %s, id: %s added to pending state", req.requestType, req.id)
		span.LogFields(log.String("event", "request moved to pending state"))
	}

	if errors.IsAlreadyExists(err) {
		node.logger.Info().Msgf("Ignoring request %v, already in pending state", req)
		span.LogFields(log.String("event", "duplicate request ignored"))
	} else if err != nil {
		node.logger.Error().Err(err).Msgf("Request with id %s, ignored because of unexpected error", req.id)
		span.LogFields(log.String("event", "request queue failure"), log.Error(err))
		ext.Error.Set(span, true)
	} else {
		// put request in queue and return reply will be sent once request is processed
		node.requestProcessorQueue <- req
		node.logger.Printf("Handle request %s, id: %s added to queue", req.requestType, req.id)
		span.LogFields(log.String("event", "request added queue"))
	}

	node.logger.Printf("Handle request %s --", req.requestType)
}

func (node *ChainReplicationNode) persistRecord(ctx context.Context, req *Request) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "persist-record")
	defer span.Finish()
	_, err := node.pending.RemoveById(req.id)
	span.LogFields(log.String("event", "request "+req.id+" removed from pending state"))

	if errors.IsNotFound(err) {
		node.logger.Info().Msgf("Request with id %s, is not in pending state for %s", req.id, node.addr)
		return
	}

	node.stableStore.Write(spanCtx, storage.Record{
		Key:   req.key,
		Value: req.value,
	})

	node.sendAck(spanCtx, req)
}

func (node *ChainReplicationNode) sendAck(ctx context.Context, request *Request) {
	if node.config.prev == "" {
		return
	}

	var reply rpcInterfaces.NoopReply
	update := rpcInterfaces.UpdateArgs{
		RequestBase: utils.NewRequestBase(),
	}

	update.RequestId = request.id
	// TODO: time out implementation
	err := utils.TraceableCall(node.config.prev, "ChainReplicationNode.SyncAck", &update, &reply, ctx)
	if err != nil {
		node.logger.Error().Msgf("Ack %s, failed for requestId: %v", node.config.prev, request.id)
	}
}

func (node *ChainReplicationNode) sendNodeAddAck(ctx context.Context, request *Request) {
	if node.config.prev == "" {
		return
	}

	span, spanCtx := opentracing.StartSpanFromContext(ctx, "send-ack")
	defer span.Finish()
	var reply rpcInterfaces.NoopReply
	reqB := utils.NewRequestBase()
	reqB.RequestId = request.id

	update := rpcInterfaces.AddNodeArgs{
		RequestBase: reqB,
	}

	// TODO: time out implementation
	err := utils.TraceableCall(node.config.prev, "ChainReplicationNode.SyncAddNode", &update, &reply, spanCtx)
	if err != nil {
		ext.Error.Set(span, true)
		span.LogFields(log.String("event", "send ack failure"), log.Error(err))
		node.logger.Error().Msgf("Ack %s, failed for requestId: %v", node.config.next, request.id)
	}
}

func (node *ChainReplicationNode) forward(ctx context.Context, request *Request) {
	if node.config.next == "" {
		return
	}
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "forward-chain-request")
	defer span.Finish()

	// making an assumption if sent results in failure then successor will eventually be removed from chain by failure detector
	// adding it sent history then makes this case similar to the case where req reached succ then succ node died.
	node.sent.Add(utils.NewChainLink(request, request.id))
	var err error
	// TODO: time out implementation
	if request.requestType == AddNode {
		node.logger.Printf("Forwarding add node request %s", request.id)
		reqB := utils.NewRequestBase()
		reqB.RequestId = request.id
		args := rpcInterfaces.AddNodeArgs{
			RequestBase: reqB,
			Addr:        request.replyAddr,
		}
		var reply rpcInterfaces.AddNodeReply
		err = utils.TraceableCall(node.config.next, "ChainReplicationNode.AddNode", &args, &reply, spanCtx)
	} else {
		node.logger.Printf("Forwarding update request %s", request.id)
		var reply rpcInterfaces.NoopReply
		update := rpcInterfaces.UpdateArgs{
			Key:          request.key,
			Value:        request.value,
			RequestBase:  utils.NewRequestBase(),
			ReplyAddress: request.replyAddr,
		}
		update.RequestId = request.id
		err = utils.TraceableCall(node.config.next, "ChainReplicationNode.SyncUpdate", &update, &reply, spanCtx)
	}

	if err != nil {
		node.logger.Error().Err(err).Msgf("Failed to forward request from %s to %s, RequestId: %v", node.addr, node.config.next, request.id)
	}
}

// handle sync acknowledgement
func (node *ChainReplicationNode) handleSyncUpdateAck(ctx context.Context, r *Request) {
	node.sent.RemoveById(r.id)
	node.persistRecord(ctx, r)
}

func (node *ChainReplicationNode) handleSyncUpdate(ctx context.Context, r *Request) {
	// end of chain, persist and send ack back
	if node.config.isTail() {
		node.persistRecord(ctx, r)
	} else {
		// keep forwarding the request until it reaches tail
		node.forward(ctx, r)
	}
}

func (node *ChainReplicationNode) getLastRequestIdReceivedBySuccessor(ctx context.Context) string {

	if node.config.next == "" {
		return ""
	}

	req := rpcInterfaces.LastRequestReceivedRequest{
		RequestBase : utils.NewRequestBase(),
	}
	var rep rpcInterfaces.LastRequestReceivedReply
	node.logger.Printf("Fetching last request received by %s", node.config.next)
	err := utils.TraceableCall(node.config.next, "ChainReplicationNode.LastRequestId", &req, &rep, ctx)
	if err != nil {
		node.logger.Error().Err(err).Msgf("Failed to fetch last request received from: %v", node.config.next)
		return ""
	}
	node.logger.Printf("Fetch complete last request received by %s, Request Id : %s", node.config.next, rep.Id)
	return rep.Id
}

func (node *ChainReplicationNode) replaySentRequest(ctx context.Context, startAtRequestId string) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "replay-sent-request")
	defer span.Finish()
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
		node.forward(spanCtx, r)
	}
}

func (node *ChainReplicationNode) handleAddNode(ctx context.Context, req *Request) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "process-add-node")
	defer span.Finish()
	if node.config.isTail() {
		ok, err := node.sendHistory(spanCtx, req)
		if ok {
			node.config.next = req.replyAddr
			span.LogFields(log.String("event", "history sync success"))
			node.sendNodeAddAck(spanCtx, req)
		} else {
			if err != nil {
				ext.Error.Set(span, true)
				span.LogFields(log.String("event", "history sync failure"), log.Error(err))
			}
		}
	} else {
		node.forward(spanCtx, req)
	}
}

func (node *ChainReplicationNode) handleAddNodeAck(ctx context.Context, req *Request) {
	node.pending.RemoveById(req.id)
	node.sent.RemoveById(req.id)
	node.sendNodeAddAck(ctx, req)
}

func (node *ChainReplicationNode) populateSpan(span opentracing.Span) {
	peerServiceName := "chain-node"
	if node.config.isTail() {
		peerServiceName = "tail"
	} else if node.config.isHead() {
		peerServiceName = "head"
	}
	ext.PeerService.Set(span, peerServiceName)
	ext.PeerAddress.Set(span, node.addr)
}
