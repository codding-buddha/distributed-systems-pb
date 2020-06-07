package dispatcher

import (
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/utils"
	"hash/crc32"
	"net"
	"net/rpc"
	"sort"
	"strconv"
)

type Dispatcher struct {
	partitions []string
	logger     *utils.Logger
	listener   net.Listener
	addr       string
	shutdown   bool
	ringKeys   []int // Sorted
	ringLookup map[int]string
	hashFn     Hash
}

type Hash func(data []byte) uint32


func New(partitions []string, logger *utils.Logger, addr string) *Dispatcher {
	return &Dispatcher{
		partitions: partitions,
		logger:     logger,
		addr:       addr,
		shutdown:   false,
		ringLookup: make(map[int]string),
		hashFn:     crc32.ChecksumIEEE,
	}
}

func (dispatcher *Dispatcher) Start() {
	replicas := 5

	for _, master := range dispatcher.partitions {
		for i := 0; i < replicas; i++ {
			hash := int(dispatcher.hashFn([]byte(strconv.Itoa(i) + master)))
			dispatcher.ringKeys = append(dispatcher.ringKeys, hash)
			dispatcher.ringLookup[hash] = master
		}
	}

	sort.Ints(dispatcher.ringKeys)
	// create ring for
	rpcs := rpc.NewServer()
	err := rpcs.Register(dispatcher)
	if err != nil {
		dispatcher.logger.Error().Err(err).Msg("Failed RPC registration")
		return
	}

	l, err := net.Listen("tcp", dispatcher.addr)
	if err != nil {
		dispatcher.logger.Fatal().Err(err).Msg("Failed to start dispatcher")
		return
	}

	dispatcher.logger.Info().Msgf("Dispatcher running at %s", dispatcher.addr)
	dispatcher.listener = l

	go func() {
		for !dispatcher.shutdown {
			conn, err := l.Accept()

			if !dispatcher.shutdown && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if !dispatcher.shutdown && err != nil {
				dispatcher.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()
}

func (dispatcher *Dispatcher) Shutdown() {
	dispatcher.shutdown = true
	dispatcher.listener.Close()
}

func (dispatcher *Dispatcher) Query(args *rpcInterfaces.QueryArgs, reply *rpcInterfaces.QueryReply) error {
	master := dispatcher.findPartition(args.Key)
	dispatcher.logger.Info().Msgf("Picked partition %s for key %s", master, args.Key)
	config := dispatcher.getChainConfig(master)
	error := utils.Call(config.Tail, "ChainReplicationNode.Query", &args, &reply)
	return error
}

func (dispatcher *Dispatcher) Update(args *rpcInterfaces.UpdateArgs, reply *rpcInterfaces.UpdateReply) error {
	master := dispatcher.findPartition(args.Key)
	dispatcher.logger.Info().Msgf("Picked partition %s for key %s", master, args.Key)
	config := dispatcher.getChainConfig(master)
	error := utils.Call(config.Head, "ChainReplicationNode.Update", &args, &reply)
	return error
}

func (dispatcher *Dispatcher) findPartition(key string) string {
	hash := int(dispatcher.hashFn([]byte(key)))
	idx := sort.Search(len(dispatcher.ringKeys), func(i int) bool {
		return dispatcher.ringKeys[i] >= hash
	})

	if idx == len(dispatcher.ringKeys) {
		idx = 0
	}

	return dispatcher.ringLookup[dispatcher.ringKeys[idx]]
}

func (dispatcher *Dispatcher) getChainConfig(master string) *rpcInterfaces.ChainConfigurationReply{
	req := rpcInterfaces.ChainConfigurationRequest{
		RequestBase: utils.NewRequestBase(),
	}

	var reply rpcInterfaces.ChainConfigurationReply

	utils.Call(master, "Master.GetChainConfiguration", &req, &reply)
	return &reply
}