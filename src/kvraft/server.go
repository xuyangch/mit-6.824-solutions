package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

const (
	checkMapItv        = 30 * time.Millisecond
	readFromApplyChItv = 100 * time.Millisecond
)

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op        string
	Key       string
	Val       string
	RequestId int64
}

type RequestState struct {
	ch    chan Result
	term  int
	index int
}
type Result struct {
	Err
	Value string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	resultByReq map[int64]Result
	reqsByIdx   map[int][]int64
	kvMap       map[string]string
	term        int
	isLeader    bool
}

func (kv *KVServer) startAndRecordCmd(cmd Op) {
	DPrintf("%v Enter startAndRecordCmd", kv.me)
	index, term, isLeader := kv.rf.Start(cmd)
	kv.isLeader = isLeader
	kv.term = term
	if !isLeader {
		DPrintf("%v Exit startAndRecordCmd", kv.me)
		return
	}
	kv.reqsByIdx[index] = append(kv.reqsByIdx[index], cmd.RequestId)
	DPrintf("Server %v (%v): reqId: %v send to raft, index is: %v", kv.me, kv.isLeader, cmd.RequestId, index)
	DPrintf("%v Exit startAndRecordCmd", kv.me)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("%v Enter Get", kv.me)
	kv.mu.Lock()
	kv.startAndRecordCmd(Op{
		Op:        "Get",
		Key:       args.Key,
		RequestId: args.Id,
	})
	kv.mu.Unlock()
	res := kv.fetchResult(args.Id)
	reply.Value = res.Value
	reply.Err = res.Err
	DPrintf("%v Exit Get", kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("%v Enter PutAppend", kv.me)
	kv.mu.Lock()
	DPrintf("Server %v (%v): reqId: %v, Receive PutAppend, args: %+v", kv.me, kv.isLeader, args.Id, args)
	kv.startAndRecordCmd(Op{
		Op:        args.Op,
		Key:       args.Key,
		Val:       args.Value,
		RequestId: args.Id,
	})
	kv.mu.Unlock()
	res := kv.fetchResult(args.Id)
	reply.Err = res.Err
	DPrintf("%v Exit PutAppend", kv.me)
}

func (kv *KVServer) fetchResult(reqId int64) Result {
	DPrintf("%v Enter fetchResult, reqId: %v", kv.me, reqId)
	defer DPrintf("%v Exit fetchResult, reqId: %v", kv.me, reqId)
	res := Result{}
	for {
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return res
		}
		if !kv.isLeader {
			res.Err = ErrWrongLeader
			kv.mu.Unlock()
			return res
		}
		res, prs := kv.resultByReq[reqId]
		if prs {
			if res.Err == ErrWrongLeader {
				delete(kv.resultByReq, reqId)
			}
			kv.mu.Unlock()
			return res
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) readFromApplyChLoop() {
	for {
		DPrintf("%v Enter new loop of readFromApplyCh", kv.me)

		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("%v waiting for applyCh completed", kv.me)
			kv.mu.Lock()
			// msg indicating change of term
			if applyMsg.Command == nil {
				if applyMsg.CommandTerm > kv.term {
					kv.isLeader = false
					kv.term = applyMsg.CommandTerm
				}
				kv.mu.Unlock()
				continue
			}
			cmd := applyMsg.Command.(Op)
			DPrintf("Server %v (%v): read command %+v from raft, index is: %v applying", kv.me, kv.isLeader, cmd, applyMsg.CommandIndex)

			// check duplicate
			tempRes, prs := kv.resultByReq[cmd.RequestId]
			if prs && tempRes.Err != ErrWrongLeader {
				// duplicate
				kv.mu.Unlock()
				continue
			}
			// apply command
			result := Result{}
			switch cmd.Op {
			case "Get":
				value, prs := kv.kvMap[cmd.Key]
				if !prs {
					result.Err = ErrNoKey
				} else {
					result.Value = value
					result.Err = OK
				}
			case "Put":
				kv.kvMap[cmd.Key] = cmd.Val
				result.Err = OK
			case "Append":
				//_, prs := kv.kvMap[cmd.Key]
				/*if !prs {
					result.Err = ErrNoKey
				} else {*/
				kv.kvMap[cmd.Key] += cmd.Val
				result.Err = OK
			}
			DPrintf("Server %v (%v): successfully apply", kv.me, kv.isLeader)
			// record result
			kv.resultByReq[cmd.RequestId] = result
			reqMatched := false

			// for all requests waiting on that index, check which request is actually applied
			for _, reqId := range kv.reqsByIdx[applyMsg.CommandIndex] {
				// request with that index but not actually applied, reply wrongleader
				if reqId != cmd.RequestId {
					DPrintf("Server %v (%v): request %v has the same index but is not applied", kv.me, kv.isLeader, reqId)
					_, prs := kv.resultByReq[reqId]
					if !prs {
						kv.resultByReq[reqId] = Result{Err: ErrWrongLeader}
					}
				} else {
					// request with that index and is actually applied, reply ok
					DPrintf("Server %v (%v): request %v has the same index and is actually applied", kv.me, kv.isLeader, reqId)
					reqMatched = true
				}
			}
			// lost the lead if term change or current index not match to any waiting req
			if applyMsg.CommandTerm > kv.term || (len(kv.reqsByIdx[applyMsg.CommandIndex]) > 0 && !reqMatched) {
				DPrintf("Server %v (%v): lost the lead!", kv.me, kv.isLeader)
				kv.term = applyMsg.CommandTerm
				kv.isLeader = false
			}
			kv.mu.Unlock()

		case <-time.After(readFromApplyChItv):
			if kv.killed() {
				return
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.reqsByIdx = make(map[int][]int64)
	kv.resultByReq = make(map[int64]Result)
	kv.kvMap = make(map[string]string)

	// You may need initialization code here.
	go kv.readFromApplyChLoop()

	return kv
}
