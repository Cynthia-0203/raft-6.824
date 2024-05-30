package kvsrv

import (
	"log"
	"sync"
	
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	// key string
	// value string
	store map[string]string
	// Your definitions here.
	clientSeq map[int64]int64
	clientResults map[int64]interface{}
	
}
func (kv *KVServer) CleanClientData(clientId int64) {
	
	
	delete(kv.clientSeq, clientId)
	delete(kv.clientResults, clientId)
	
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("收到 Get 请求: %v", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("已获取 Get 锁")
	if seq, ok := kv.clientSeq[args.ClientId]; ok && seq >= args.SeqNum {
		// if result, ok := kv.clientResults[args.ClientId].(GetReply); ok {
		// 	*reply = result
		// 	return
		// }
		if seq < args.SeqNum {
			delete(kv.clientResults, args.ClientId)
		} else if result, ok := kv.clientResults[args.ClientId].(GetReply); ok {
			*reply = result
			return
		}
		
	}
	

	if value, ok := kv.store[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	kv.clientSeq[args.ClientId] = args.SeqNum
	kv.clientResults[args.ClientId] = *reply
	kv.CleanClientData(args.ClientId)
	DPrintf("Get 响应: %v", reply)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("收到 Put 请求: %v", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("已获取 Put 锁")
	if seq, ok := kv.clientSeq[args.ClientId]; ok && seq >= args.SeqNum {
		// if result, ok := kv.clientResults[args.ClientId].(PutAppendReply); ok {
		// 	*reply = result
		// 	return
		// }
		
		if seq < args.SeqNum {
			delete(kv.clientResults, args.ClientId)
		} else if result, ok := kv.clientResults[args.ClientId].(PutAppendReply); ok {
			*reply = result
			return
		}
		
	}
	
	kv.store[args.Key] = args.Value

	kv.clientSeq[args.ClientId] = args.SeqNum
	kv.clientResults[args.ClientId] = *reply
	kv.CleanClientData(args.ClientId)
	DPrintf("Put 响应: %v", reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("收到 Append 请求: %v", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("已获取 Append 锁")
	if seq, ok := kv.clientSeq[args.ClientId]; ok && seq >= args.SeqNum {
		// if result, ok := kv.clientResults[args.ClientId].(PutAppendReply); ok {
		// 	*reply = result
		// 	return
		// }
		if seq < args.SeqNum {
			delete(kv.clientResults, args.ClientId)
		} else if result, ok := kv.clientResults[args.ClientId].(PutAppendReply); ok {
			*reply = result
			return
		}
	}

	if value, ok := kv.store[args.Key]; ok {
		kv.store[args.Key] = value + args.Value
		reply.Value=value
	} else {
		kv.store[args.Key] = args.Value
		reply.Value=""
	}

	kv.clientSeq[args.ClientId] = args.SeqNum
	kv.clientResults[args.ClientId] = *reply
	kv.CleanClientData(args.ClientId)
	DPrintf("Append 响应: %v", reply)
}

func StartKVServer() *KVServer {

	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.clientSeq = make(map[int64]int64)
	kv.clientResults = make(map[int64]interface{})
	return kv
}

