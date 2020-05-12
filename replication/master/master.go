package master

import (
	rpcdefinition "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	chain      *utils.Chain
	nodeLookup map[string]*Node
	lock       sync.RWMutex
	logger     utils.Logger
	listener net.Listener
	alive      bool
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

func StartMaster(addr string, logger utils.Logger) (*Master, error) {
	master := &Master{
		chain:      utils.NewChain(),
		lock:       sync.RWMutex{},
		logger:     logger,
		alive:      true,
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

func (master *Master) Register(args *rpcdefinition.NodeRegistrationArgs, reply *rpcdefinition.NodeRegistrationReply) error {
	master.logger.Info().Msgf("Received registration request: %v", args)
	node := NewNode(args.Address)
	prev, _ := master.add(node)

	reply.IsTail = true
	reply.IsHead = prev == &emptyNode

	reply.Next = ""
	if *prev != emptyNode {
		master.logger.Info().Msgf("New tail in the chain, sending update to predecessor %s", prev.Addr)
		reply.Previous = prev.Addr
		go master.notifyChainUpdate(prev)
	}

	master.logger.Info().Msg("Registration complete")

	return nil
}

func (master *Master) GetChainConfiguration(args *rpcdefinition.ChainConfigurationRequest, reply *rpcdefinition.ChainConfigurationReply) error {
	master.logger.Info().Msg("Got configuration fetch request")
	head := master.head()
	tail := master.tail()
	reply.Head = head.Addr
	reply.Tail = tail.Addr
	return nil
}

func (master *Master) Heartbeat(args *rpcdefinition.HeartBeatArgs, reply *rpcdefinition.NoopReply) error {
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

func (master *Master) notifyChainUpdate(node *Node) {
	if *node == emptyNode {
		return
	}

	chainLink, _ := master.chain.GetById(node.Addr)
	nextNode := master.getNode(chainLink.Next())
	prevNode := master.getNode(chainLink.Previous())

	updateConfigArgs := rpcdefinition.SyncConfigurationRequest{
		Next: nextNode.Addr,
		Previous: prevNode.Addr,
	}

	var reply rpcdefinition.SyncConfigurationReply
	// update predecessor about new tail
	err := utils.Call(node.Addr, "ChainReplicationNode.SyncConfig", &updateConfigArgs, &reply)
	if err != nil {
		master.logger.Error().Err(err).Msgf("Failed to notify chain link updates to node %s", node.Addr)
	}
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

		master.chain.IterFunc(func (chainLink *utils.ChainLink) {
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

		// Update nodes
		for _, chainLink := range linksToBeUpdated {
			node := master.getNode(chainLink)
			master.logger.Printf("Node to be updated %s", node.Addr)
			master.logger.Printf("Prev: %s, Next: %s",
				master.getNode(chainLink.Previous()).Addr,
				master.getNode(chainLink.Next()).Addr)
			if node.alive {
				master.notifyChainUpdate(node)
			}
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

func(master *Master) logChainLink(chainLink *utils.ChainLink) {
	//master.logger.Printf("Data: %v, Next : %v, Prev : %v", chainLink.Data(), chainLink.Previous(), chainLink.Next())
}
