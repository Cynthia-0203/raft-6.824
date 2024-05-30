package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"

	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}



type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for{

	
		var nReduce int
		args1 := AskReduceNumArgs{}
		reply1 := AskReduceNumReply{}
		ok := call("Coordinator.ReduceNum", &args1, &reply1)
		if !ok {
			
			return
		}
		nReduce = reply1.ReduceNum

		args := AskTaskArgs{}
		reply := AskTaskReply{}
		reply.Task = ""
		reply.StartReduce = false
		ok = call("Coordinator.Asktask", &args, &reply)
		if !ok {
		
			return
		}
		

		if reply.Task != "" {
			// read file
			filename := reply.Task
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			// write to intermediate files

			buckets := make([][]KeyValue, nReduce)
			for i := 0; i < nReduce; i++ {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range kva {
				buckets[ihash(kv.Key)%nReduce] = append(buckets[ihash(kv.Key)%nReduce], kv)
			}

			for i, bucket := range buckets {
				oname := "mr-" + strings.Replace(reply.Task[3:(len(reply.Task)-4)], "-", "_", -1) + "-" + strconv.Itoa(i)
				ofile, _ := os.CreateTemp("", oname)
				enc := json.NewEncoder(ofile)
				for _, kv := range bucket {
					if err := enc.Encode(&kv); err != nil {
						log.Fatalf("cannot write %v", oname)
					}
				}
				os.Rename(ofile.Name(), oname)
			}

			// send func map success message
			mapSuccessArgs := MapSuccessArgs{}
			mapSuccessArgs.Task = reply.Task
			mapSuccessReply := MapSuccessReply{}
			_ = call("Coordinator.MapSuccess", &mapSuccessArgs, &mapSuccessReply)
		} else if reply.StartReduce {
			// start to reduce
			args2 := AskReduceArgs{}
			reply2 := AskReduceReply{}
			reply2.ReduceNum = -1
			ok := call("Coordinator.AskReduce", &args2, &reply2)
			
			if !ok {
				
				return
			}
			if reply2.ReduceNum < 0 {
				time.Sleep(1 * time.Second)
				
				continue
			}
			reply2.ReduceNum -= 1
			reduceNum := reply2.ReduceNum
			dir := "./"
			suffix := strconv.Itoa(reduceNum)
			kva := []KeyValue{}
			intermediate := []KeyValue{}
			
			_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && strings.HasSuffix(path, suffix) {
					file, _ := os.Open(path)
					defer file.Close()
					
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							if err == io.EOF {
								break
							}
							//fmt.Printf("dec.Decode(&kv) error\n")
							break
						}
						
						kva = append(kva, kv)
					}
				}
				return nil
			})
			intermediate = append(intermediate, kva...)
		
			

			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(reduceNum)
			ofile, _ := os.CreateTemp("", oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

			os.Rename(ofile.Name(), oname)
			//send reduce success message
			reduceSuccessArgs := ReduceSuccessArgs{}
			reduceSuccessReply := ReduceSuccessReply{}
			reduceSuccessArgs.ReduceNum = reply2.ReduceNum
			_ = call("Coordinator.ReduceSuccess", &reduceSuccessArgs, &reduceSuccessReply)

		} else {
			//wait for a period of time
			time.Sleep(1 * time.Second)

		}
	}
	
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	
	args := ExampleArgs{}

	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}





