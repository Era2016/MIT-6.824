package kvraft

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/m/labgob"
	"6.824/m/labrpc"
	"6.824/m/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug != 0 {
		logger := log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
		logger.Printf(format, a...)
		//log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	CommandId int64

	Res Response
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	stateMachine   StateMachine
	lastOperations map[int64]Op           // clientId => operations, recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *Response // index => chan response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{Operation: GET, Key: args.Key, ClientId: args.ClientId})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resp := kv.waitResponse(index)
	reply = (*resp).(*GetReply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.lastOperations[args.ClientId].CommandId == args.CommandId {
		resp := kv.lastOperations[args.ClientId].Res
		*reply = (resp).(PutAppendReply)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{Operation: GET, Key: args.Key, ClientId: args.ClientId, CommandId: args.CommandId})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resp := kv.waitResponse(index)
	reply = (*resp).(*PutAppendReply)
}

func (kv *KVServer) waitResponse(index int) *Response {

	kv.mu.Lock()
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *Response)
	}
	ch := kv.notifyChans[index]
	kv.mu.Unlock()

	var resp *Response
	defer func() {
		kv.mu.Lock()
		close(ch)
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()

	timer := time.NewTimer(time.Second * 5)
	for {
		select {
		case resp = <-ch:
			return resp
		case <-timer.C:
			return nil
		}
	}
}

func (kv *KVServer) applyToStateMachine() {
	for !kv.killed() {

		var reps Response
		for msg := range kv.applyCh {
			if !msg.CommandValid {
				continue
			}

			kv.mu.Lock()
			if kv.lastApplied >= msg.CommandIndex {
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)
			if op.Operation == GET {
				reply := GetReply{}
				reply.Value, reply.Err = kv.stateMachine.Get(op.Key)
				reps = reply
			} else {
				// filter again
				if kv.lastOperations[op.ClientId].CommandId == op.CommandId {
					reps = kv.lastOperations[op.ClientId].Res
					goto NOTIFY
				}
				if op.Operation == PUT {
					reply := PutAppendReply{}
					reply.Err = kv.stateMachine.Put(op.Key, op.Value)
					reps = reply
				} else {
					reply := PutAppendReply{}
					reply.Err = kv.stateMachine.Append(op.Key, op.Value)
					reps = reply
				}

				op.Res = reps
				kv.lastOperations[op.ClientId] = op
			}
			kv.lastApplied = msg.CommandIndex
		NOTIFY:
			ch := kv.notifyChans[msg.CommandIndex]
			kv.mu.Unlock()

			ch <- &reps
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

	// You may need initialization code here.

	kv.lastOperations = make(map[int64]Op)
	kv.notifyChans = make(map[int]chan *Response)
	kv.stateMachine = NewMemoryKV()

	go kv.applyToStateMachine()
	return kv
}
