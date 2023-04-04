package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/m/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	clientId      int64
	commandId     int64
	currentLeader int64
	rw            sync.RWMutex
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

	ck.clientId = nrand()
	ck.commandId = 0
	//ck.currentLeader = nrand() % int64(len(ck.servers))
	ck.currentLeader = 0
	DPrintf("MakeClerk[%d] starts...", ck.clientId)
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("[client][%d] starts Get: [key: %s, op: %s]", ck.clientId, key, "Get")
	args := GetArgs{Key: key, ClientId: ck.clientId, CommandId: ck.commandId}
	reply := GetReply{}

	ck.rw.RLock()
	serverId := ck.currentLeader
	ck.rw.RUnlock()
	for {
		if ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply); !ok || reply.Err != OK {
			if reply.Err != "OK" {
				DPrintf("[client][%d] Clerk.Get failed, err: %v", ck.clientId, reply.Err)
			}

			serverId = (serverId + 1) % int64(len(ck.servers))
			continue
		} else {
			ck.rw.Lock()
			ck.currentLeader = serverId
			DPrintf("[client][%d] Get.update currentLeader, new: %d", ck.clientId, ck.currentLeader)
			ck.rw.Unlock()
			break
		}
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	DPrintf("[client][%d] starts PutAppend: [key: %s, value: %s, op: %s]", ck.clientId, key, value, op)
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, CommandId: ck.commandId}
	reply := PutAppendReply{}

	ck.rw.RLock()
	serverId := ck.currentLeader
	ck.rw.RUnlock()
	for {
		DPrintf("[client][%d] calls server[%d] PutAppend(%s,%s,%s): ", ck.clientId, serverId, key, value, op)
		if ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply); !ok || reply.Err != OK {
			if reply.Err != "OK" {
				DPrintf("[client][%d] Clerk.PutAppend failed, err: %v", ck.clientId, reply.Err)
			}

			serverId = (serverId + 1) % int64(len(ck.servers))
			continue
		} else {
			ck.rw.Lock()
			ck.currentLeader = serverId
			ck.commandId++
			DPrintf("[client][%d] PutAppend.update currentLeader, new: %d", ck.clientId, ck.currentLeader)
			ck.rw.Unlock()
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
