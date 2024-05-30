package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce int
	mutex sync.Mutex
	reduceIndex int
	files []string
	map1 map[int]bool//false 未完成且计时不到10s true 未完成且计时已过10s
	mapIndex int
	reducePhase bool //指示是否进入 reduce 阶段。
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func(c *Coordinator)ReduceNum(args *AskReduceNumArgs,reply *AskReduceNumReply)error{
	// c.mutex.Lock()
	// defer c.mutex.Unlock()
	reply.ReduceNum=c.nReduce
	return nil
}

//分配 reduce 任务。如果还有未分配的 reduce 任务，就分配一个新的任务；如果有超时的任务，则重新分配该任务
func(c *Coordinator)AskReduce(args *AskReduceArgs,reply *AskReduceReply)error{
	// c.mutex.Lock()
	// defer c.mutex.Unlock()
	if c.reduceIndex<c.nReduce{
		reply.ReduceNum=c.reduceIndex+1
		c.map1[c.reduceIndex]=false
		i:=c.reduceIndex
		go c.waitWorker(i)
		c.reduceIndex++
	}else if len(c.map1)>0{
		key:=-1
		for k,v:=range c.map1{
			if v{
				key=k
				break
			}
		}
		if key>=0{
			// reduce 任务编号从 1 开始
			reply.ReduceNum=key+1
			c.map1[key]=false
			i:=key
			go c.waitWorker(i)
		}
	}

	return nil
}
//reply.ReduceNum 表示分配给 worker 的 reduce 任务编号。任务编号从 0 开始，但在给 worker 分配任务时，通常会把编号从 1 开始（为了与其他系统保持一致或者为了让编号更具可读性）

func(c *Coordinator)Asktask(args *AskTaskArgs,reply *AskTaskReply)error{
	// c.mutex.Lock()
	// defer c.mutex.Unlock()
	if c.reducePhase{
		reply.StartReduce=true
		return nil
	}

	if c.mapIndex<len(c.files){
		reply.Task=c.files[c.mapIndex]
		c.map1[c.mapIndex]=false
		i:=c.mapIndex
		go c.waitWorker(i)
		c.mapIndex++
	}else if len(c.map1)>0{
		key:=-1
		for k,v :=range c.map1{
			if v{
				key=k
				break
			}
		}
		if key>=0{
			reply.Task=c.files[key]
			c.map1[key]=false
			i:=key
			go c.waitWorker(i)
		}
	}else{
		c.reducePhase=true
		reply.StartReduce=true
	}
	return nil
}

func (c *Coordinator) MapSuccess(args *MapSuccessArgs, reply *MapSuccessReply) error {
	// c.mutex.Lock()
	// defer c.mutex.Unlock()
	var i int
	for i = 0; i < len(c.files); i++ {
		if c.files[i] == args.Task {
			break
		}
	}
	delete(c.map1, i)
	return nil
}

func (c *Coordinator) ReduceSuccess(args *ReduceSuccessArgs, reply *ReduceSuccessReply) error {
	// c.mutex.Lock()
	// defer c.mutex.Unlock()
	delete(c.map1, args.ReduceNum)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {

	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Coordinator server started at %s\n", sockname) // 添加日志信息
	go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		http.DefaultServeMux.ServeHTTP(w, r)
	}))
}

func(c *Coordinator)waitWorker(mapId int){
	time.Sleep(10*time.Second)
	c.mutex.Lock()
    defer c.mutex.Unlock()
	if _, exists := c.map1[mapId]; exists {
        c.map1[mapId] = true
    }
}
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// c.mutex.Lock()
    // defer c.mutex.Unlock()
	// Your code here.
	return c.reducePhase && c.reduceIndex == c.nReduce && len(c.map1) == 0
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapIndex=0
	c.reduceIndex = 0
	c.files = files
	c.reducePhase = false
	c.nReduce = nReduce
	c.map1 = make(map[int]bool)
	c.server()
	return &c
}

