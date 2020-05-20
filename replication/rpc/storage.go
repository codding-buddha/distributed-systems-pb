package rpc

import "github.com/codding-buddha/ds-pb/utils"

type ResponseCallback struct {
	Key       string
	Value     string
	utils.RequestBase
	Error     string
}

type UpdateArgs struct {
	Key          string
	Value        string
	ReplyAddress string
	utils.RequestBase
}

type UpdateReply struct {
	Ok bool
}

type QueryArgs struct {
	Key          string
	ReplyAddress string
	utils.RequestBase
}

type QueryReply struct {
	Key   string
	Value string
	Ok    bool
}
