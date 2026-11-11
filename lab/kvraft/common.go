package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrUndef       = "ErrUndef"
)

type Err string

type KVArgs struct {
	Key   string
	Value string
	Op    string
}

type KVReply struct {
	Err   Err
	Value string
}
