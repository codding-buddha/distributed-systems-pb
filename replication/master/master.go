package master

import (
	"context"
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	addr       string
	chain      *utils.Chain
	nodeLookup map[string]*Node
	lock       sync.RWMutex
	logger     utils.Logger
	listener   net.Listener
	alive      bool
	weakChain  bool
}

type Node struct {
	LastSeen time.Time
	alive    bool
	Addr     string
}

func NewNode(addr string) *Node {

	return &Node{
		LastSeen: time.Now(),
		alive:    true,
		Addr:     addr,
	}
}

var emptyNode = Node{
	LastSeen: time.Time{},
	alive:    false,
	Addr:     "",
}

func StartMaster(addr string, logger utils.Logger, weakChain bool) (*Master, error) {
	master := &Master{
		chain:  utils.NewChain(),
		lock:   sync.RWMutex{},
		logger: logger,
		alive:  true,
		addr:   addr,
		weakChain: weakChain,
	}

	rpcs := rpc.NewServer()
	err := rpcs.Register(master)
	if err != nil {
		logger.Error().Err(err).Msg("Failed RPC registration")
		return master, err
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start master")
		return master, err
	}

	logger.Info().Msgf("Master running at %s", addr)
	master.listener = l

	go func() {
		for master.alive {
			conn, err := l.Accept()

			if master.alive && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if master.alive && err != nil {
				master.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()

	go master.updateAliveNodes()
	return master, nil
}

func (master *Master) add(node *Node) (*Node, error) {
	chainLink := utils.NewChainLink(node, node.Addr)
	prev, err := master.chain.Add(chainLink)
	if err != nil || prev.IsNull() {
		return &emptyNode, err
	}

	prevNode := master.getNode(prev)
	return prevNode, nil
}

func (master *Master) head() *Node {
	c := master.chain.Head()
	node := master.getNode(c)
	return node
}

func (master *Master) tail() *Node {
	c := master.chain.Tail()
	node := master.getNode(c)
	return node
}

func (master *Master) Register(args *rpcInterfaces.NodeRegistrationStartArgs, reply *rpcInterfaces.NodeRegistrationReply) error {
	span, closer := args.CreateServerSpan("chain-extension", master.populateSpan)
	defer closer()

	master.logger.Info().Msgf("Received registration request: %v", args)
	node := NewNode(args.Address)
	defer master.lock.Unlock()
	master.lock.Lock()
	if master.chain.IsEmpty() {
		master.logger.Info().Msgf("First node in chain, short circuiting the registration flow")
		master.add(node)
		reply.AddedToChain = true
		span.LogFields(log.String("event", "chain extended"), log.String("chain.length", string(master.chain.Count())))
		return nil
	}

	err := master.extendChain(node, opentracing.ContextWithSpan(context.Background(), span))

	if err != nil {
		ext.Error.Set(span, true)
		span.LogFields(log.String("event", "error"), log.Error(err))
	}

	return err
}

func (master *Master) NodeReady(args *rpcInterfaces.NodeReadyArgs, reply *rpcInterfaces.NodeRegistrationReply) error {
	_, closer := args.CreateServerSpan("chain-extension-complete", master.populateSpan)
	defer closer()
	master.logger.Printf("NodeReady ++")
	node := NewNode(args.Addr)
	defer master.lock.Unlock()
	master.lock.Lock()
	prev, _ := master.add(node)
	reply.AddedToChain = true
	reply.Next = ""
	reply.Previous = ""
	if prev != &emptyNode {
		reply.Previous = prev.Addr
	}
	master.logger.Printf("NodeReady --")
	return nil
}

func (master *Master) GetChainConfiguration(args *rpcInterfaces.ChainConfigurationRequest, reply *rpcInterfaces.ChainConfigurationReply) error {
	_, closer := args.CreateServerSpan("get-chain-configuration", master.populateSpan)
	defer closer()

	master.logger.Info().Msg("Got configuration fetch request")
	head := master.head()
	tail := master.tail()
	reply.Head = head.Addr
	if master.weakChain {
		reply.Tail = master.pickRandomNode()
	} else {
		reply.Tail = tail.Addr
	}

	return nil
}

func (master *Master) pickRandomNode() string {
	rand.Seed(time.Now().UnixNano())
	indx := rand.Intn(master.chain.Count())
	curr := 0
	addr := ""
	master.chain.IterFunc(func(chain *utils.ChainLink) {
		if indx == curr {
			addr = chain.Id()
		}
		curr++
	})
	return addr
}

func (master *Master) Heartbeat(args *rpcInterfaces.HeartBeatArgs, reply *rpcInterfaces.NoopReply) error {
	chainLink, ok := master.chain.GetById(args.Sender)

	if ok {
		chainLink.Data().(*Node).LastSeen = time.Now()
	} else {
		master.logger.Printf("Received heart beat from non-active node %s", args.Sender)
	}

	return nil
}

func (master *Master) Shutdown() {
	master.alive = false
	master.listener.Close()
}

func (master *Master) notifyChainUpdate(ctx context.Context, node *Node, fReqId string, lReqId string) (string, string) {
	if *node == emptyNode {
		return "nil", "nil"
	}

	chainLink, _ := master.chain.GetById(node.Addr)
	nextNode := master.getNode(chainLink.Next())
	prevNode := master.getNode(chainLink.Previous())

	updateConfigArgs := rpcInterfaces.SyncConfigurationRequest{
		Next:                  nextNode.Addr,
		Previous:              prevNode.Addr,
		FirstPendingRequestId: fReqId,
		LastPendingRequestId:  lReqId,
		RequestBase:           utils.NewRequestBase(),
	}

	var reply rpcInterfaces.SyncConfigurationReply
	// update predecessor about new tail
	err := utils.TraceableCall(node.Addr, "ChainReplicationNode.SyncConfig", &updateConfigArgs, &reply, ctx)
	if err != nil {
		master.logger.Error().Err(err).Msgf("Failed to notify chain link updates to node %s", node.Addr)
	}

	return reply.FirstPendingRequestId, reply.LastPendingRequestId
}

func (master *Master) updateAliveNodes() {
	threshold := time.Second * 5
	interval := time.Second * 3
	updateCheck := time.NewTicker(interval)

	for range updateCheck.C {

		if !master.alive {
			break
		}

		now := time.Now()
		// enumerate all nodes that are assumed to be alive, if nodes didn't send any sample/heartbeat then update the chain
		var linksToBeUpdated []*utils.ChainLink
		var linksToBeRemoved []*utils.ChainLink

		master.chain.IterFunc(func(chainLink *utils.ChainLink) {
			node := master.getNode(chainLink)
			if node != &emptyNode && node.alive {
				// remove node from chain if not heard from it within a check cycle
				if node.LastSeen.Add(threshold).Before(now) {
					master.logger.Printf("Node last seen : %v, Current time : %v, total time : %v", node.LastSeen, now, node.LastSeen.Add(threshold))
					master.logger.Printf("Removing node %s from chain", node.Addr)
					node.alive = false

					// notify successor node of failed node first
					if !chainLink.Next().IsNull() {
						linksToBeUpdated = append(linksToBeUpdated, chainLink.Next())
						master.logger.Printf("Nodes to be updated %v", linksToBeUpdated)
					}

					if !chainLink.Previous().IsNull() {
						linksToBeUpdated = append(linksToBeUpdated, chainLink.Previous())
						master.logger.Printf("Nodes to be updated %v", linksToBeUpdated)
					}

					linksToBeRemoved = append(linksToBeRemoved, chainLink)
				} else {
					master.logger.Printf("Node %s is alive.", node.Addr)
				}
			}
		})

		// Remove failed nodes from chain
		for _, chainLink := range linksToBeRemoved {
			master.chain.Remove(chainLink)
		}

		trace := len(linksToBeUpdated) > 0
		var ctx context.Context
		var span opentracing.Span
		if trace {
			tracer := opentracing.GlobalTracer()
			span = tracer.StartSpan("chain-reconfiguration")
			ctx = opentracing.ContextWithSpan(context.Background(), span)
		}

		// Update nodes
		lReqId := "nil"
		fReqId := "nil"
		for _, chainLink := range linksToBeUpdated {
			node := master.getNode(chainLink)
			master.logger.Printf("Node to be updated %s", node.Addr)
			master.logger.Printf("Prev: %s, Next: %s",
				master.getNode(chainLink.Previous()).Addr,
				master.getNode(chainLink.Next()).Addr)
			if node.alive {
				fReqId, lReqId = master.notifyChainUpdate(ctx, node, fReqId, lReqId)
			}
		}

		if span != nil {
			span.Finish()
		}
	}
}

func (master *Master) getNode(chainLink *utils.ChainLink) *Node {
	if chainLink == nil || chainLink.IsNull() {
		return &emptyNode
	}

	//master.logger.Printf("Data -> %v", chainLink.Data())
	return chainLink.Data().(*Node)
}

func (master *Master) logChainLink(chainLink *utils.ChainLink) {
	//master.logger.Printf("Data: %v, Next : %v, Prev : %v", chainLink.Data(), chainLink.Previous(), chainLink.Next())
}

func (master *Master) extendChain(node *Node, ctx context.Context) error {
	args := rpcInterfaces.AddNodeArgs{
		RequestBase: utils.NewRequestBase(),
		Addr:        node.Addr,
	}

	var reply rpcInterfaces.AddNodeReply
	err := utils.TraceableCall(master.head().Addr, "ChainReplicationNode.AddNode", &args, &reply, ctx)
	return err
}

func (master *Master) populateSpan(span opentracing.Span) {
	ext.PeerService.Set(span, "master")
	ext.PeerAddress.Set(span, master.addr)
}
