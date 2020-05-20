package rpc

import (
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
)

type NodeRegistrationStartArgs struct {
	Address string
	utils.RequestBase
}

type NodeRegistrationReply struct {
	Previous     string
	Next         string
	AddedToChain bool
}

type SyncConfigurationRequest struct {
	Previous string
	Next     string
	utils.RequestBase
}

type SyncConfigurationReply struct {
	Ok bool
}

type ChainConfigurationReply struct {
	Head string
	Tail string
}

type ChainConfigurationRequest struct {
	utils.RequestBase
}

type NoopReply struct {
}

type SyncHistoryArgs struct {
	History []storage.Record
	Sender  string
	utils.RequestBase
}

type SyncHistoryReply struct {
	Ok bool
}

type NodeReadyArgs struct {
	Addr string
	utils.RequestBase
}

type AddNodeArgs struct {
	Addr string
	utils.RequestBase
}

type AddNodeReply struct {
	Ok bool
}

type ContextCarrier struct {

}
