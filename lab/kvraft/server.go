package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	// "time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Cmd struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op   string
	Args interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store  map[string]string
	notifs map[int]chan RaftResult
}

type RaftResult struct {
	err Err
	val string
}

func (kv *KVServer) L() {
	kv.mu.Lock()
}
func (kv *KVServer) Unl() {
	kv.mu.Unlock()
}

func (kv *KVServer) KVRequest(args *KVArgs, reply **KVReply) {

	cmd := Cmd{
		Op:   args.Op,
		Args: args.asSlice(),
	}

	kv.L()
	idx, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		*reply = &KVReply{
			Err: ErrWrongLeader,
		}
		kv.Unl()
		return
	}

	notification := make(chan RaftResult, 1)
	kv.notifs[idx] = notification
	kv.Unl()

	log.Printf("%d %s waiting confirmation %d", kv.me, args.Op, idx-1)
	select {
	case rr := <-notification:
		*reply = &KVReply{
			Err:   rr.err,
			Value: rr.val,
		}
		// add case handle timeouts
	}

	kv.L()
	delete(kv.notifs, idx)
	kv.Unl()
}

func (a *KVArgs) asSlice() []string {
	switch a.Op {
	case "Get":
		return []string{a.Key}
	default:
		return []string{a.Key, a.Value}
	}
}

func (kv *KVServer) raftLoop() {

	for {
		select {
		case msg := <-kv.applyCh:

			kv.L()
			log.Printf("%d ApplyMsg %v", kv.me, msg)
			cmd := msg.Command.(Cmd)
			a := cmd.Args.([]string)
			var val string
			var e Err = OK
			switch cmd.Op {
			case "Put":
				kv.store[a[0]] = a[1]
			case "Append":
				kv.store[a[0]] = kv.store[a[0]] + a[1]
			case "Get":
				v, ok := kv.store[a[0]]
				val = v
				if !ok {
					e = ErrNoKey
				}
			}

			ch, hasCh := kv.notifs[msg.CommandIndex]
			kv.Unl()

			if hasCh {

				rr := RaftResult{
					err: e,
					val: val,
				}

				ch <- rr
				close(ch)
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	log.Printf("%d ending...", kv.me)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Cmd{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.notifs = make(map[int]chan RaftResult)

	kv.applyCh = make(chan raft.ApplyMsg, applyChSize)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.raftLoop()

	return kv
}

// applyChSize is the buffer size of the channel used by the Raft library.
// In a real world scenario this buffer should be scaled according
// to an expected number of clients, to prevent stalling the
// Raft library.
const applyChSize int = 10
