package replication

import (
	"github.com/codding-buddha/ds-pb/utils"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	nodes []Node
	lock  sync.RWMutex
	logger utils.Logger
	alive bool
}

type Node struct {
	LastSeen time.Time
	Addr string
}

var emptyNode Node = Node {}

func (master *Master) tail() Node {
	defer master.lock.RUnlock()
	master.lock.RLock()
	if len(master.nodes) > 0 {
		return master.nodes[len(master.nodes) - 1]
	}

	return  emptyNode
}

func (master *Master) head() Node {
	defer master.lock.RUnlock()
	master.lock.RLock()
	if len(master.nodes) > 0 {
		return master.nodes[0]
	}

	return  emptyNode
}

func (master *Master) add(node Node) Node {
	prev := emptyNode
	defer master.lock.Unlock()
	master.lock.Lock()
	if len(master.nodes) > 0 {
		prev = master.nodes[len(master.nodes) - 1]
	}
	master.nodes = append(master.nodes, node)
	return prev
}

func (master *Master) count() int {
	defer master.lock.RUnlock()
	master.lock.RLock()
	return len(master.nodes)
}

type NodeRegistrationArgs struct {
	Address string
}

type NodeRegistrationReply struct {
	IsHead bool
	IsTail bool
	Previous string
	Next string
}

type ChainConfigurationReply struct {
	Head string
	Tail string
}

type ChainConfigurationRequest struct {

}

func StartMaster(addr string, logger utils.Logger) {
	master := &Master{
		nodes: []Node{},
		lock: sync.RWMutex{},
		logger: logger,
		alive: true,
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

	for master.alive {
		conn, err := l.Accept()

		if master.alive && err == nil {
			rpcs.ServeConn(conn)
			conn.Close()
		} else if master.alive == false && err != nil {
			master.logger.Error().Err(err).Msg("Accept failure")
		}
	}
}

func (master *Master) Register(args *NodeRegistrationArgs, reply *NodeRegistrationReply) error {
	master.logger.Info().Msgf("Received registration request: %v", args)
	node := Node{
		LastSeen: time.Now(),
		Addr:     args.Address,
	}

	prev := master.add(node)

	reply.IsTail = true
	reply.IsHead = master.count() == 1
	reply.Next = ""
	if prev != emptyNode {
		reply.Previous = prev.Addr
	}

	master.logger.Info().Msg("Registration complete")
	// TODO: update predecessor about new tail
	return nil
}

func (master *Master) GetChainConfiguration(args *ChainConfigurationRequest, reply *ChainConfigurationReply) error {
	master.logger.Info().Msg("Got configuration fetch request")
	head := master.head()
	tail := master.tail()
	reply.Head = head.Addr
	reply.Tail = tail.Addr
	return nil
}