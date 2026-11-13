package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "log"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	return ck.send(key, "", "Get")
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	_ = ck.send(key, value, op)
}

const requestTimeout time.Duration = 500 * time.Millisecond

func (ck *Clerk) send(key string, value string, op string) string {

	args := KVArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Idem:  newIdemKey(),
	}

	var i int = ck.lastLeader
	var result string
	var err Err = ErrUndef
	var networkFailures int

	for err != OK && err != ErrNoKey {
		log.Printf("sending %s to server %d %v", op, i, args)

		requestEnded := make(chan bool)
		reply := KVReply{Err: ErrUndef}
		go func() {
			requestEnded <- ck.servers[i].Call("KVServer.KVRequest", &args, &reply)
			close(requestEnded)
		}()

		select {
		case networkOk := <-requestEnded:

			if !networkOk {
				networkFailures += 1
				break
			}

			result = reply.Value
			err = reply.Err
			log.Printf("%s response %s", op, err)

		case <-time.After(requestTimeout):
			networkFailures += 1
		}

		if networkFailures > 5 || reply.Err == ErrWrongLeader {
			networkFailures = 0
			i = (i + 1) % len(ck.servers)
			log.Printf("changing leader %d", i)
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
	ck.lastLeader = i
	log.Printf("%s round ended", op)

	return result
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func newIdemKey() string {
	return randstring(8)
}
