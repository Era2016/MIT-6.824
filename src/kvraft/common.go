package kvraft

import (
	"log"
	"os"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

const (
	GET       = "Get"
	PUT       = "Put"
	APPEND    = "Append"
	PUTAPPEND = "putappend"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientId  int64
	CommandId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientId  int64
	CommandId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type Response interface {
}

type StateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{KV: make(map[string]string)}
}

func (m *MemoryKV) Get(key string) (string, Err) {
	if _, ok := m.KV[key]; ok {
		return m.KV[key], OK
	}

	return "", ErrNoKey
}

func (m *MemoryKV) Put(key, value string) Err {
	m.KV[key] = value
	return OK
}

func (m *MemoryKV) Append(key, value string) Err {
	m.KV[key] += value
	return OK
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug != 0 {
		logger := log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
		logger.Printf(format, a...)
		//log.Printf(format, a...)
	}
	return
}
