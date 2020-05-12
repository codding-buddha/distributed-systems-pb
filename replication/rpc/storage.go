package rpc


type ResponseCallback struct {
	Key       string
	Value     string
	RequestId string
	Error     string
}


type UpdateArgs struct {
	Key          string
	Value        string
	RequestId    string
	ReplyAddress string
}

type UpdateReply struct {
	Ok bool
}

type QueryArgs struct {
	Key          string
	RequestId    string
	ReplyAddress string
}

type QueryReply struct {
	Ok bool
}