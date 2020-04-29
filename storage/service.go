package storage

import (
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"net"
	"net/rpc"
)

type LookupService struct {
	db       *KeyValueStore
	addr     string
	listener net.Listener
	alive    bool
	logger   *utils.Logger
	closed   chan interface{}
}

func (lookupService *LookupService) Start() {
	rpcs := rpc.NewServer()
	rpcs.Register(lookupService)
	l, err := net.Listen("tcp", lookupService.addr)
	if err != nil {
		lookupService.logger.Fatal().Err(err).Msg("Failed to start service")
		return
	}

	lookupService.listener = l
	go func() {
		for lookupService.alive {
			conn, err := lookupService.listener.Accept()

			if lookupService.alive && err == nil {
				rpcs.ServeConn(conn)
				conn.Close()
			} else if lookupService.alive == false && err != nil {
				lookupService.logger.Error().Err(err).Msg("Accept failure")
			}
		}
	}()
}

func (lookupService *LookupService) Shutdown() {
	lookupService.logger.Info().Msg("Shutting down")
	lookupService.alive = false
	lookupService.listener.Close()
	lookupService.db.Close()
	close(lookupService.closed)
}

func InitLookupService(db *KeyValueStore, addr string, logger *utils.Logger) *LookupService {
	l := &LookupService{
		db:       db,
		addr:     addr,
		listener: nil,
		alive:    true,
		logger:   logger,
		closed:   make(chan interface{}),
	}

	return l
}

func Run(store *KeyValueStore, addr string, logger *utils.Logger) *LookupService {
	l := InitLookupService(store, addr, logger)
	l.Start()
	return l
}

func (lookupService *LookupService) OnClose() <-chan interface{} {
	return lookupService.closed
}

func (lookupService *LookupService) Get(args *GetRecordArgs, reply *GetRecordReply) error {
	lookupService.logger.Printf("get request received, key: %s", args.Key)
	r, err := lookupService.db.Get(args.Key)
	reply.Record = r
	reply.Ok = err != nil
	reply.NotFound = errors.IsNotFound(err)

	// its okay if not found
	if !errors.IsNotFound(err) {
		return err
	}

	return nil
}

func (lookupService *LookupService) Write(args *WriteRecordArgs, reply *WriteRecordReply) error {
	lookupService.logger.Printf("write request received, key: %s, value : %s", args.Record.Key, args.Record.Value)
	err := lookupService.db.Write(*args.Record)
	reply.Ok = err != nil
	return err
}

func (lookupService *LookupService) Kill(args *ShutdownServiceArgs, reply *ShutdownServiceReply) error {
	lookupService.Shutdown()
	return nil
}

type GetRecordArgs struct {
	Key string
}

type GetRecordReply struct {
	Record   Record
	Ok       bool
	NotFound bool
}

type WriteRecordArgs struct {
	Record *Record
}

type WriteRecordReply struct {
	Ok bool
}

type ShutdownServiceArgs struct {
}

type ShutdownServiceReply struct {
}
