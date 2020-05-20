package rpc

import "github.com/codding-buddha/ds-pb/utils"

type HeartBeatArgs struct {
	Sender string
}

type LastRequestReceivedReply struct {
	Id string
}

type LastRequestReceivedRequest struct {
	utils.RequestBase
}
