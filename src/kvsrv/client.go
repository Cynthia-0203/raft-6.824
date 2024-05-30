package kvsrv

import (
	"crypto/rand"
	"math/big"

	"sync"

	// "sync"
	"time"

	"6.5840/labrpc"
)


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	clientID int64
	seqNum int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientID=nrand()
	// You'll have to add code here.
	return ck
}

func (ck *Clerk)nextSeqNum()int64{
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// seqNum:=ck.seqNum
	ck.seqNum++
	return ck.seqNum
}
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key: key,
		ClientId: ck.clientID,
		SeqNum: ck.nextSeqNum(),
	}
	reply := &GetReply{}

	for {
		if ok := ck.server.Call("KVServer.Get", args, reply); ok {
			return reply.Value
		}
		DPrintf("Failed to Get")
		time.Sleep(10 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	
	args := &PutAppendArgs{
		Key: key,
		Value: value,
		ClientId: ck.clientID,
		SeqNum: ck.nextSeqNum(),
	}
	reply := &PutAppendReply{}

	for{
		if ok := ck.server.Call("KVServer."+op, args, reply); ok {
			return reply.Value
		}
		DPrintf("Failed to Put or Append")
		// You will have to modify this function.
		time.Sleep(10*time.Millisecond)
	}
	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
