package rpc

type NodeRegistrationArgs struct {
	Address string
}

type NodeRegistrationReply struct {
	IsHead   bool
	IsTail   bool
	Previous string
	Next     string
}

type SyncConfigurationRequest struct {
	Previous string
	Next string
}

type SyncConfigurationReply struct {
	Ok bool
}

type ChainConfigurationReply struct {
	Head string
	Tail string
}

type ChainConfigurationRequest struct {
}

type NoopReply struct {
}
