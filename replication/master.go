package replication

import (
	"github.com/codding-buddha/ds-pb/utils"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	chain      Chain
	nodeLookup map[string]*Node
	lock       sync.RWMutex
	logger     utils.Logger
	alive      bool
}

type Node struct {
	LastSeen time.Time
	alive    bool
	Addr     string
	Next     *Node
	Prev     *Node
}

type Chain struct {
	head *Node
	tail *Node
	l    sync.RWMutex
}

var emptyNode = Node{
	LastSeen: time.Time{},
	alive:    false,
	Addr:     "",
	Next:     nil,
	Prev:     nil,
}

func NewNode(addr string) *Node {

	return &Node{
		LastSeen: time.Now(),
		alive:    true,
		Addr:     addr,
		Next:     &emptyNode,
		Prev:     &emptyNode,
	}
}

func (node *Node) isHead() bool {
	return node != &emptyNode && node.Prev == &emptyNode
}

func (node *Node) isTail() bool {
	return node != &emptyNode && node.Next == &emptyNode
}

func (master *Master) tail() *Node {
	return master.chain.tail
}

func (master *Master) head() *Node {
	return master.chain.head
}

func (chain *Chain) isEmpty() bool {
	return *chain.head == emptyNode || *chain.tail == emptyNode
}

func (master *Master) add(node *Node) *Node {
	return master.chain.add(node)
}

func (chain *Chain) add(node *Node) *Node {
	prev := &emptyNode

	defer chain.l.Unlock()
	chain.l.Lock()

	if chain.isEmpty() {
		chain.tail = node
		chain.head = node
	} else {
		prev = chain.tail
		prev.Next = node
		node.Prev = prev
		chain.tail = node
	}

	return prev
}

func (chain *Chain) removeNode(node *Node) {
	prev := node.Prev
	next := node.Next

	if prev != nil && *prev != emptyNode {
		prev.Next = next
	}

	if next != nil && *next != emptyNode {
		next.Prev = prev
	}

	if node == chain.head {
		chain.head = next
	}

	if node == chain.tail {
		chain.tail = prev
	}

	node.Prev = nil
	node.Next = nil
}

type NodeRegistrationArgs struct {
	Address string
}

type NodeRegistrationReply struct {
	IsHead   bool
	IsTail   bool
	Previous string
	Next     string
}

type ChainConfigurationReply struct {
	Head string
	Tail string
}

type ChainConfigurationRequest struct {
}

func StartMaster(addr string, logger utils.Logger) {
	master := &Master{
		chain: Chain{
			head: &emptyNode,
			tail: &emptyNode,
			l:    sync.RWMutex{},
		},
		lock:       sync.RWMutex{},
		logger:     logger,
		alive:      true,
		nodeLookup: map[string]*Node{},
	}

	rpcs := rpc.NewServer()
	err := rpcs.Register(master)
	if err != nil {
		logger.Error().Err(err).Msg("Failed RPC registration")
		return
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start master")
		return
	}

	logger.Info().Msgf("Master running at %s", addr)

	go func() {
		for master.alive {
			conn, err := l.Accept()

			if master.alive && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if master.alive == false && err != nil {
				master.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()

	go master.updateAliveNodes()
}

func (master *Master) Register(args *NodeRegistrationArgs, reply *NodeRegistrationReply) error {
	master.logger.Info().Msgf("Received registration request: %v", args)
	node := NewNode(args.Address)

	prev := master.add(node)

	reply.IsTail = node.isTail()
	reply.IsHead = node.isHead()
	reply.Next = ""
	if *prev != emptyNode {
		master.logger.Info().Msgf("New tail in the chain, sending update to predecessor %s", prev.Addr)
		reply.Previous = prev.Addr
		master.notifyChainUpdate(prev)
	}

	master.lock.Lock()
	master.nodeLookup[args.Address] = node
	master.lock.Unlock()

	master.logger.Info().Msg("Registration complete")

	return nil
}

func (master *Master) GetChainConfiguration(args *ChainConfigurationRequest, reply *ChainConfigurationReply) error {
	master.logger.Info().Msg("Got configuration fetch request")
	defer master.lock.RUnlock()
	master.lock.RLock()
	head := master.head()
	tail := master.tail()
	reply.Head = head.Addr
	reply.Tail = tail.Addr
	return nil
}

func (master *Master) Heartbeat(args *HeartBeatArgs, reply *NoopReply) error {
	node, ok := master.nodeLookup[args.Sender]
	if ok {
		node.LastSeen = time.Now()
	} else {
		master.logger.Printf("Received heart beat from non-active node %s", args.Sender)
	}

	return nil
}

func (master *Master) notifyChainUpdate(node *Node) {
	if *node == emptyNode {
		return
	}

	updateConfigArgs := ChainLinkUpdate{
		Next: node.Next.Addr,
		Prev: node.Prev.Addr,
	}
	var reply NoopReply
	// update predecessor about new tail
	err := utils.Call(node.Addr, "ChainNode.UpdateTail", &updateConfigArgs, &reply)
	if err != nil {
		master.logger.Error().Err(err).Msgf("Failed to notify chain link updates to node %s", node.Addr)
	}
}

func (master *Master) updateAliveNodes() {
	threshold := time.Second * 3
	updateCheck := time.NewTicker(threshold)

	for range updateCheck.C {

		if !master.alive {
			break
		}

		now := time.Now()
		// enumerate all nodes that are assumed to be alive, if nodes didn't send any sample/heartbeat then update the chain
		var nodesToUpdated []*Node
		master.chain.l.Lock()

		for _, node := range master.nodeLookup {
			// remove node from chain if not heard from it within a check cycle
			if node.LastSeen.Add(threshold).Before(now) {
				master.logger.Printf("Removing node %s from chain", node.Addr)
				node.alive = false
				if *node.Prev != emptyNode {
					nodesToUpdated = append(nodesToUpdated, node.Prev)
					master.logger.Printf("Nodes to be updated %v", nodesToUpdated)
				}

				if *node.Next != emptyNode {
					nodesToUpdated = append(nodesToUpdated, node.Next)
					master.logger.Printf("Nodes to be updated %v", nodesToUpdated)
				}

				master.chain.removeNode(node)
				delete(master.nodeLookup, node.Addr)
			} else {
				master.logger.Printf("Node %s is alive.", node.Addr)
			}
		}

		master.chain.l.Unlock()

		// Update nodes
		for _, node := range nodesToUpdated {
			master.logger.Printf("Node to be updated %s, Prev: %s, Next: %s", node.Addr, node.Prev, node.Next)
			if node.alive {
				master.notifyChainUpdate(node)
			}
		}
	}
}
