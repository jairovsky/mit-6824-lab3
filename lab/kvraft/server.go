package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Idem string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store        map[string]string
	notifs       map[int]*sync.Cond
	idems        map[string]RaftResult
	idemsByIndex map[int]*RaftResult
}

type RaftResult struct {
	err   Err
	val   string
	index int
	idem  string
	// TODO add expiration time / background goroutine to collect expired items
}

func (kv *KVServer) L() {
	kv.mu.Lock()
}
func (kv *KVServer) Unl() {
	kv.mu.Unlock()
}

func disagree() *KVReply {
	return errorReply(ErrDisagree)
}

func errorReply(e Err) *KVReply {
	return &KVReply{
		Err: e,
	}
}
func resultReply(rr *RaftResult) *KVReply {
	return &KVReply{
		Err:   rr.err,
		Value: rr.val,
	}
}

func newRaftResult(index int) RaftResult {
	return RaftResult{
		err:   ErrUndef,
		val:   "",
		index: index,
		idem:  "",
	}
}

// done indicates whether this RaftResult has been fulfilled
func (a *RaftResult) done() bool {
	return a.err != ErrUndef
}

// matchIdem checks whether a certain index position
// has the given idempotency key
func (kv *KVServer) matchIdem(index int, idemKey string) bool {
	resultAtIdx, exists := kv.idemsByIndex[index]
	return exists && resultAtIdx.idem == idemKey
}

func (a *KVArgs) asSlice() []string {
	switch a.Op {
	case "Get":
		return []string{a.Key}
	default:
		return []string{a.Key, a.Value}
	}
}

const agreementTimeout time.Duration = 1000 * time.Millisecond

func (kv *KVServer) KVRequest(args *KVArgs, reply **KVReply) {

	done := make(chan *KVReply, 1)
	go kv.processRequest(args, done)
	*reply = <-done
}

func (kv *KVServer) processRequest(args *KVArgs, done chan *KVReply) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		done <- errorReply(ErrWrongLeader)
		return
	}

	kv.L()
	defer kv.Unl()

	var index int
	resultAtKey, exists := kv.idems[args.Idem]
	if exists {

		index = resultAtKey.index

		if resultAtKey.done() {
			done <- resultReply(kv.idemsByIndex[index])
			return
		}
	} else {
		cmd := Cmd{
			Op:   args.Op,
			Args: args.asSlice(),
			Idem: args.Idem,
		}

		var isLeader bool
		index, _, isLeader = kv.rf.Start(cmd)

		if !isLeader {
			done <- errorReply(ErrWrongLeader)
			return
		}

		kv.idems[args.Idem] = newRaftResult(index)
		kv.notifs[index] = sync.NewCond(&sync.Mutex{})
	}

	lastIndex := kv.rf.LastLogIndex()
	if lastIndex < index {
		log.Printf("%d this server has discarded entries", kv.me)
		if notification, exists := kv.notifs[index]; exists {
			notification.Broadcast()
		}
		delete(kv.idems, args.Idem)
		delete(kv.notifs, index)
		done <- disagree()
		return
	}

	go kv.waitAgreement(index, args, done)
}

// waitIndex checks if there's ongoing work happening on an index
// and waits for it to finish, otherwise returns immediately.
func (kv *KVServer) waitIndex(index int) {
	kv.L()
	notification, exists := kv.notifs[index]
	if exists {
		notification.L.Lock()
		kv.Unl()
		notification.Wait()
		notification.L.Unlock()
	} else {
		kv.Unl()
	}
}

func (kv *KVServer) waitAgreement(
	index int, args *KVArgs, done chan *KVReply) {

	log.Printf("%d %s waiting confirmation %d", kv.me, args.Op, index-1)

	finished := make(chan bool, 1)

	go func() {

		kv.waitIndex(index)

		log.Printf("%d %s confirmed %d", kv.me, args.Op, index-1)

		kv.L()
		defer kv.Unl()

		if !kv.matchIdem(index, args.Idem) {
			delete(kv.idems, args.Idem)
			done <- disagree()
			finished <- true
			return
		}

		done <- resultReply(kv.idemsByIndex[index])
		finished <- true
	}()

	select {
	case <-finished:
		return
	case <-time.After(agreementTimeout):
		done <- errorReply(ErrTimeout)
		return
	}
}

func (kv *KVServer) raftLoop() {

	for {
		select {
		case msg := <-kv.applyCh:

			kv.L()

			log.Printf("%d ApplyMsg %v", kv.me, msg)
			cmd := msg.Command.(Cmd)
			result, exists := kv.idems[cmd.Idem]
			a := cmd.Args.([]string)
			var val string
			var e Err = OK

			if !(exists && result.done()) {
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

				result = newRaftResult(msg.CommandIndex)
				result.err = e
				result.val = val
				result.idem = cmd.Idem

				kv.idems[cmd.Idem] = result
			}

			kv.idemsByIndex[msg.CommandIndex] = &result

			notification, exists := kv.notifs[msg.CommandIndex]
			kv.Unl()

			if exists {

				notification.Broadcast()

				kv.L()
				delete(kv.notifs, msg.CommandIndex)
				kv.Unl()
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
	kv.notifs = make(map[int]*sync.Cond)
	kv.idems = make(map[string]RaftResult)
	kv.idemsByIndex = make(map[int]*RaftResult)

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
