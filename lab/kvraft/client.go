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

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}
	var err Err = ErrUndef
	reply := GetReply{}
	var i int = ck.lastLeader
	for err != OK && err != ErrNoKey {
		log.Printf("sending Get to server %d %v", i, args)
		ck.servers[i].Call("KVServer.Get", &args, &reply)
		err = reply.Err
		log.Printf("Get response %s", err)
		if reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
	ck.lastLeader = i
	log.Printf("Get round ended")

	return reply.Value
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

	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	var err Err = ErrUndef
	reply := PutAppendReply{}
	var i int = ck.lastLeader
	for err != OK {
		log.Printf("sending PutAppend to server %d %v", i, args)
		ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		err = reply.Err
		log.Printf("PutAppend response %s", err)

		if reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
	ck.lastLeader = i
	log.Printf("PutAppend round ended")
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
