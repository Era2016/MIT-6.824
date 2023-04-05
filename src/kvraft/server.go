package kvraft

import (
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"6.824/m/labgob"
	"6.824/m/labrpc"
	"6.824/m/raft"
)

const reponseInterval = 600 * 1000

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	CommandId int64
}

type OpRelated struct {
	CommandId int64
	Res       Response
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
	lastOperations map[int64]OpRelated    // clientId => operations, recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *Response // index => chan response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer DPrintf("[server][%d] FuncGet starts, args: key(%s)-clientId(%d)-commandId(%d), reply: %v",
		kv.me, args.Key, args.ClientId, args.CommandId, reply)
	index, _, isLeader := kv.rf.Start(Op{Operation: GET, Key: args.Key, ClientId: args.ClientId, CommandId: args.CommandId})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resp := kv.waitResponse(index)
	if resp == nil {
		reply.Err = ErrTimeout
	} else {
		//DPrintf("[server][%d] FuncGet receive response, type: %v, value: %v", kv.me, reflect.TypeOf(*resp).Name(), *resp)
		r := (*resp).(GetReply)
		*reply = r
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer DPrintf("[server][%d] FuncPutAppend args: key(%s)-value(%s)-op(%s)-clientId(%d)-commandId(%d), reply: %v",
		kv.me, args.Key, args.Value, args.Op, args.ClientId, args.CommandId, reply)
	kv.mu.Lock()

	if op, ok := kv.lastOperations[args.ClientId]; ok && op.CommandId >= args.CommandId {
		resp := kv.lastOperations[args.ClientId].Res
		*reply = (resp).(PutAppendReply)
		kv.mu.Unlock()
		DPrintf("[server][%d] FuncPutAppend return resp from history, commandId: [%d-%d]", kv.me, op.CommandId, args.CommandId)
		return
	}

	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, CommandId: args.CommandId})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resp := kv.waitResponse(index)
	if resp == nil {
		reply.Err = ErrTimeout
	} else {
		//DPrintf("[server][%d] FuncPutAppend receive response, type: %v, value: %v", kv.me, reflect.TypeOf(*resp).Name(), *resp)
		r := (*resp).(PutAppendReply)
		*reply = r
	}
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

	for {
		select {
		case resp = <-ch:
			return resp
		case <-time.After(time.Microsecond * reponseInterval):
			return nil
		}
	}
}

func (kv *KVServer) applyToStateMachine() {

	for !kv.killed() {
		var reps Response
		select {
		case msg := <-kv.applyCh:

			//DPrintf("[server][%d] <<<<<<kv.lastOperations: %v", kv.me, kv.lastOperations)

			js, _ := json.Marshal(msg.Command)
			DPrintf("[server][%d] statemachine receive new msg, index: %d, valid: %t, content: %v", kv.me, msg.CommandIndex, msg.CommandValid, string(js))
			if !msg.CommandValid {
				continue
			}

			kv.mu.Lock()
			if kv.lastApplied >= msg.CommandIndex {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			op := msg.Command.(Op)
			if op.Operation == GET {
				reply := GetReply{}
				reply.Value, reply.Err = kv.stateMachine.Get(op.Key)
				reps = reply
			} else {
				// filter again
				//DPrintf("[server] <<<<<<kv.lastOperations: %v ,op.ClientId: %d,op.CommandId: %d", kv.lastOperations, op.ClientId, op.CommandId)
				if val, ok := kv.lastOperations[op.ClientId]; ok && val.CommandId == op.CommandId {
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

				kv.lastOperations[op.ClientId] = OpRelated{CommandId: op.CommandId, Res: reps}
				DPrintf("[server][%d] statemachine adds records for lastOperations, curr stat: %v", kv.me, kv.lastOperations)
			}

		NOTIFY:

			if term, isLeader := kv.rf.GetState(); isLeader && term == msg.CommandTerm {
				if ch, ok := kv.notifyChans[msg.CommandIndex]; ok {
					DPrintf("[server][%d] statemachine returns to the client, resp: %v, type: %v", kv.me, reps, reflect.TypeOf(reps).Name())
					ch <- &reps
				} else {
					DPrintf("[server][%d] statemachine returns to the none client", kv.me)
				}
			} else {
				DPrintf("[server][%d] statemachine discards outdated msg", kv.me)
			}
			kv.mu.Unlock()

		} // end of select
	} // end of for
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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

	kv.lastOperations = make(map[int64]OpRelated)
	kv.notifyChans = make(map[int]chan *Response)
	kv.stateMachine = NewMemoryKV()

	go kv.applyToStateMachine()
	DPrintf("StartKVServer[%d] starts...", kv.me)
	return kv
}
