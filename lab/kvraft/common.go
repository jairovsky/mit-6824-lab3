package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDisagree    = "ErrDisagree"
	ErrTimeout     = "ErrTimeout"
	ErrUndef       = "ErrUndef"
)

type Err string

type KVArgs struct {
	Key   string
	Value string
	Op    string
	Idem  string // idempotency key to detect duplicate requests
}

type KVReply struct {
	Err   Err
	Value string
}
