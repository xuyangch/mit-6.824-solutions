package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"../labrpc"
)

const (
	retryItv = 10 * time.Millisecond
)

var cnt int64

func init() {
	cnt = 0
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeader int64
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
	DPrintf("A new clerk made!")
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
	reqId := atomic.AddInt64(&cnt, 1)
	args := &GetArgs{
		Id:  reqId,
		Key: key,
	}
	reply := &GetReply{}
	sId := ck.prevLeader
	DPrintf("Clerk: reqId: %v, get new Get request (key: %v), server ID: %v", reqId, key, sId)
	for {
		ck.servers[sId].Call("KVServer.Get", args, reply)
		DPrintf("Clerk: reqId: %v, sID: %v, Get RPC (key: %v) result: %+v", reqId, sId, key, reply)
		if reply.Err == OK || reply.Err == ErrNoKey {
			ck.prevLeader = sId
			break
		}
		sId = nrand() % int64(len(ck.servers))
		time.Sleep(retryItv)
	}
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
	// You will have to modify this function.
	reqId := atomic.AddInt64(&cnt, 1)
	args := &PutAppendArgs{
		Id:    reqId,
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := &PutAppendReply{}
	sId := ck.prevLeader
	DPrintf("Clerk: reqId: %v, gets new PutAppend (key: %v, value: %v, op: %v), server ID: %v", reqId, key, value, op, sId)
	for {
		ck.servers[sId].Call("KVServer.PutAppend", args, reply)
		DPrintf("Clerk: reqId: %v, sID: %v, PutAppend (key: %v, value: %v, op: %v) result: %v", reqId, sId, key, value, op, reply)
		if reply.Err == OK || reply.Err == ErrNoKey {
			ck.prevLeader = sId
			break
		}
		sId = nrand() % int64(len(ck.servers))
		time.Sleep(retryItv)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
