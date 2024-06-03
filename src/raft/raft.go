package raft

//
// 这是 Raft 必须向服务（或测试器）公开的 API 大纲。有关每个函数的更多详细信息，请参阅下面的注释。
//
// rf = Make(...)
//   创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   开始就新的日志条目达成一致。
// rf.GetState() (term, isLeader)
//   请求 Raft 的当前任期和其是否认为自己是领导者。
// ApplyMsg
//   每当新条目被提交到日志时，每个 Raft 对等体都应将 ApplyMsg 发送到服务（或测试器）中的相同服务器。
//

import (
	//	"bytes"
	// "math"
	// "math/rand"

	"fmt"
	
	// "sort"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// 每当每个 Raft 对等体意识到连续的日志条目已经提交时，该对等体应通过传递给 Make() 的 applyCh 向相同服务器上的服务（或测试器）发送 ApplyMsg。将 CommandValid 设置为 true，以指示 ApplyMsg 包含一个新提交的日志条目。
//
// 在第 3D 部分，您可能希望在 applyCh 上发送其他类型的消息（例如快照），但是对于这些其他用途，请将 CommandValid 设置为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const(
	LEADER = iota
	FOLLOWER 
	CANDIDATE
	TermError
	PreLogIndexError
	TermNotMatch
	IndexNotExist
)

// 心跳间隔和选举超时范围
const(
	HeartbeatInterval = 100 * time.Millisecond
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 800 * time.Millisecond
)


type Entry struct{
	Index int
	
	Term int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	state int
	currentTerm int
	votedFor int
	heartbeatTimer time.Time
	electionTimer time.Time
	log []Entry
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	
	applyCh chan ApplyMsg
	applyCond *sync.Cond
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// astIncludeIndex  int         // snapshot保存的最后log的index
	// lastIncludeTerm   int         // snapshot保存的最后log的term

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term=rf.currentTerm
	isleader=(rf.state==LEADER)
	return term, isleader
}

// 将 Raft 的持久状态保存到稳定存储中，以便在崩溃和重启后可以恢复。
// 有关需要持久化的内容，请参见论文的图 2。
// 在实现快照之前，你应该传递 nil 作为 persister.Save() 的第二个参数。
// 在实现快照之后，传递当前的快照（如果还没有快照，则传递 nil）。
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// 恢复之前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// 服务表示它已经创建了一个包含所有信息的快照，直至并包括指定的索引。
// 这意味着服务不再需要该索引（及其之前）的日志。
// Raft 现在应该尽可能地裁剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// 示例 RequestVote RPC 参数结构。
// 字段名称必须以大写字母开头！
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogItem int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term<rf.currentTerm{
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		// 重置自身的状态
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	
	
	// if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
	// 	// candidate的日志必须比我的新
	// 	// 1, 最后一条log，任期大的更新
	// 	// 2，更长的log则更新
	// 	currentLogIndex:=len(rf.log)-1
	// 	currentLogTerm := rf.log[len(rf.log)-1].Term
		
	// 	if args.LastLogItem < currentLogTerm || args.LastLogIndex < currentLogIndex  {
	// 		reply.VoteGranted=false
	// 		reply.Term = rf.currentTerm
	// 		return
	// 	}

	// 	rf.votedFor = args.CandidateID
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = true
		
	// }else{
	// 	reply.VoteGranted=false
	// 	if rf.votedFor != args.CandidateID {
	// 		return
	// 	}else{
	// 		rf.state=FOLLOWER
	// 	}
	// }

	// rf.resetElectionTimer()
	// rf.persist()
	
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
        lastLogIndex := len(rf.log) - 1
        lastLogTerm := rf.log[lastLogIndex].Term
        if (args.LastLogItem > lastLogTerm) || (args.LastLogItem == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
            rf.votedFor = args.CandidateID
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false
        }
    } else {
        reply.VoteGranted = false
    }

	reply.Term = rf.currentTerm
    if reply.VoteGranted {
        rf.resetElectionTimer()
    }
}

type AppendEntryArgs struct{
	Term int
	LeaderId int
	PreLogIndex int
	PreLogItem int
	Entries []Entry
	LeaderCommit int
}

type AppendEntryReply struct{
	Term int
	Success bool
	ConflictTerm int
	ConflictIndex int
	CommitIndex int
	AppendError int
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
   	// rf.mu.Lock()
   	// defer rf.mu.Unlock()
	// fmt.Printf("node%v in appendEntries\n",rf.me)
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	//this node's term is greater than leader's node 
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
		reply.AppendError=TermError
		fmt.Printf("node%v false1\n",rf.me)
		fmt.Printf("args.Term:%v rf.currentTerm:%v\n",args.Term,rf.currentTerm)
        return
    }
	rf.resetElectionTimer()
    //this node's term is less than leader's node
	//reset this node's term
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
		//reset votedFor
        rf.votedFor = -1
		rf.state = FOLLOWER
		
    }

	if args.Entries==nil{
		// fmt.Println("true")
	}else{
		if args.PreLogIndex<rf.getFirstLogIndex(){
			fmt.Println("false2")
			reply.Success=false
			reply.AppendError=PreLogIndexError
			return
		}
		// fmt.Printf("args.PrevLogIndex:%v\n",args.PreLogIndex)
		// fmt.Printf("args.PrevLogTerm:%v\n",args.PreLogItem)
		// fmt.Printf("rf.log:%v\n",rf.log)
		if _,ok:=rf.getByIndex(args.PreLogIndex);!ok{
			reply.Success=false
			fmt.Println("false3")
			reply.AppendError=IndexNotExist
			reply.ConflictIndex=rf.lastLogIndex()+1
			fmt.Printf("reply.conficIndex:%v\n",reply.ConflictIndex)
			// fmt.Println("false3")
			return
		}
		
		if index,ok:=rf.getByIndex(args.PreLogIndex);(ok&&rf.log[index].Term!=args.PreLogItem){
			reply.Term=rf.currentTerm
			reply.Success=false
			reply.AppendError=TermNotMatch
			firstIndex:=rf.getFirstLogIndex()
			reply.ConflictTerm=rf.log[args.PreLogIndex-firstIndex].Term
			index:=args.PreLogIndex-1
			for index >= firstIndex&&rf.log[index-firstIndex].Term==reply.ConflictTerm{
				index--
			}
			reply.ConflictIndex=index+1
			fmt.Println("false1")
			return
		}
	}

	
	
	firstIndex := rf.getFirstLogIndex()
	for _,entry:=range args.Entries{
		if entry.Index-firstIndex>=len(rf.log)||rf.log[entry.Index-firstIndex].Term!=entry.Term{
			// rf.log=shrinkEntriesArray(append(rf.log[:entry.Index-firstIndex],entry))
			rf.log = append(rf.log[:entry.Index-firstIndex], entry)
			// fmt.Printf("len:%v\n",len(rf.log[:entry.Index-firstIndex]))
			rf.commitIndex = entry.Index
			// fmt.Printf("node%v,commitIndex:%v\n",rf.me,rf.commitIndex)
			// fmt.Printf("node%v append log%v\n",rf.me,rf.log)
			break
		}
	}	
	// fmt.Printf("node%v is follower,start apply\n",rf.me)
	rf.applyLogs()
	reply.Term=rf.currentTerm
	reply.Success=true
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("node :%v,rf.lastApplied:%v,rf.commitIndex:%v\n",rf.me,rf.lastApplied,rf.commitIndex)
    for rf.lastApplied < rf.commitIndex {
		// fmt.Printf("node%v's rf.log in applier:%v\n",rf.me,rf.log)
		
        rf.lastApplied++
		// fmt.Printf("rf.lastApplied:%v\n",rf.lastApplied)
        applyMsg := ApplyMsg{
            CommandValid: true,
            CommandIndex: rf.lastApplied,
            Command:      rf.log[rf.lastApplied].Command,
        }
		fmt.Printf("node%v apply msg:%v\n",rf.me,applyMsg.CommandIndex)
        rf.applyCh <- applyMsg
		
		// fmt.Printf("node%v success\n",rf.me)
    }
	time.Sleep(50*time.Millisecond)
}


func (rf *Raft) getByIndex(index int) (int, bool) {
	
	
	i, j := 0, len(rf.log)-1
	for i <= j {
		mid := (i+j)/2
		if rf.log[mid].Index > index {
			j = mid
		} else if rf.log[mid].Index < index{
			i = mid + 1
		}else{
			return mid,true
		}
	}
	return 0,false
}

func(rf *Raft)getFirstLogIndex()int{
	
	return rf.log[0].Index
}

// func(rf *Raft)getLastLog()Entry{
// 	return rf.log[len(rf.log)-1]
// }




// 示例代码，发送 RequestVote RPC 到服务器。
// server 是目标服务器在 rf.peers[] 中的索引。
// RPC 参数在 args 中。
// 用 RPC 回复填充 *reply，所以调用者应该传递 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须与处理函数中声明的参数类型相同（包括是否是指针）。
//
// labrpc 包模拟一个丢包网络，其中服务器可能不可达，请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果在超时间隔内收到回复，Call() 返回 true；否则，
// Call() 返回 false。因此 Call() 可能需要一段时间才会返回。
// false 的返回可能是由于服务器故障、无法到达的活动服务器、请求丢失或回复丢失引起的。
//
// Call() 保证会返回（可能会有延迟），*除非*服务器端的处理函数没有返回。因此
// 无需在 Call() 周围实现你自己的超时机制。
//
// 更多细节请查看 ../labrpc/labrpc.go 中的注释。
//
// 如果你在使 RPC 工作时遇到问题，检查你是否已将通过 RPC 传递的结构体中的所有字段名称都大写，
// 并且调用者是否传递了回复结构体的地址（使用 &），而不是传递结构体本身。

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
// 使用Raft的服务（例如一个键值服务器）希望在Raft的日志中开始对下一个命令的共识。
// 如果这个服务器不是领导者，则返回false。
// 否则，开始共识并立即返回。由于领导者可能会失败或失去选举，因此不能保证这个命令会被提交到Raft的日志中。
// 即使Raft实例已经被关闭，这个函数也应该优雅地返回。
//
// 第一个返回值是该命令如果被提交后将出现的索引。
// 第二个返回值是当前的任期。
// 第三个返回值是一个布尔值，如果该服务器认为自己是领导者，则返回true。

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	if rf.killed() {
		fmt.Println("node killed")
		return index, term, false
	}

	

	// 如果不是leader，直接返回
	if rf.state != LEADER{
		// fmt.Printf("node%v isn't leader\n",rf.me)
		return index, term, false
	}
	
	isLeader = true

	// 初始化日志条目。并进行追加
	appendLog := Entry{
		Index: len(rf.log),
		Term: rf.currentTerm, 
		Command: command,
	}
	rf.log = append(rf.log, appendLog)
	// fmt.Printf("leader: node%v,current log:%v\n",rf.me,rf.log)
	index = len(rf.log)-1
	term = rf.currentTerm

	return index, term, isLeader
}


// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) ticker() {
    for !rf.killed() {
	
		now := time.Now()
        if rf.state == LEADER {
			if now.After(rf.heartbeatTimer) {
				// fmt.Printf("node%v start heartbeat\n",rf.me)
				rf.sendHeartbeats()
				rf.resetHeartbeatTimer()
			}
		} else {
			if now.After(rf.electionTimer) {
				// fmt.Printf("node%v start election\n",rf.me)
				rf.startElection()
				rf.resetElectionTimer()
			}
		}
        time.Sleep(10 * time.Millisecond)
    }
}

func (rf *Raft) startElection() {
    
	rf.mu.Lock()
	defer rf.mu.Unlock()
    rf.state = CANDIDATE
    rf.currentTerm++
    rf.votedFor = rf.me


    currentTerm := rf.currentTerm
    lastLogIndex := 0
    lastLogTerm := 0

    if len(rf.log) > 1 {
        lastLogIndex = rf.log[len(rf.log)-1].Index
        lastLogTerm = rf.log[len(rf.log)-1].Term
    }
	// fmt.Printf("node%v beacome candidate\n",rf.me)
	// fmt.Printf("candidate's lastLogIndex:%v,lastLogTerm:%v\n",lastLogIndex,lastLogTerm)
    
	
    votes := int32(1)
    
	rf.resetElectionTimer()
    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        go func(i int) {
			// rf.mu.Lock()
			// defer rf.mu.Unlock()
            args := &RequestVoteArgs{
                Term:         currentTerm,
                CandidateID:  rf.me,
                LastLogIndex: lastLogIndex,
                LastLogItem:  lastLogTerm,
            }
            reply := &RequestVoteReply{}
			ok:=rf.sendRequestVote(i, args, reply)
			
            if  ok{
                rf.mu.Lock()
                defer rf.mu.Unlock()
                if reply.VoteGranted {
                    votes++
					// fmt.Printf("node%v gain %v granted\n",rf.me,votes)
                    if votes > int32(len(rf.peers)/2) {
						
                        if rf.state == CANDIDATE && rf.currentTerm == currentTerm {
							rf.state = LEADER
							// fmt.Printf("node%v become leader\n",rf.me)
                            rf.sendHeartbeats()
                        }
                    }
                } else if reply.Term > currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = FOLLOWER
                    rf.votedFor = -1
                }
            }
        }(i)
    }
}



func (rf *Raft) sendHeartbeats() {
    
	if rf.state != LEADER{
		// fmt.Printf("node%v isn't leader\n",rf.me)
		return
	}
	// fmt.Printf("node%v start send heartbeats for peers\n",rf.me)
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
		
		// fmt.Printf("%v start send heartbeats,current term is %v\n",rf.me,rf.currentTerm)
		go func(server int) {
			
			args:=&AppendEntryArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PreLogIndex: 0,
				PreLogItem: 0,
				Entries:  nil,
				LeaderCommit: rf.commitIndex,
			}
			
			//代表已经不是初始值0
			if rf.nextIndex[server] > 1 {
				args.PreLogIndex = rf.nextIndex[server]-1
			}
	
			if args.PreLogIndex > 0 {
				args.PreLogItem = rf.log[args.PreLogIndex].Term
			}
			nextIndex:=rf.nextIndex[server]
			args.Entries = rf.log[nextIndex:]
			// fmt.Printf("node%v,entries:%v\n",server,args.Entries)
			fmt.Printf("node%v args.entries:%v\n",server,args.Entries)
			reply := &AppendEntryReply{}
			// fmt.Printf("args:%v\n",args)
			// fmt.Printf("%v start append entries,prevLogIndex is %v,append entries is %v\n",server,args.PreLogIndex,args.Entries)
			ok := rf.sendAppendEntry(server, args, reply)

			if !ok {
				// rpc error
				// fmt.Println("rpc error")
				return
			}
			// fmt.Printf("node %v' append entries reply %v\n",server,*reply)
			// fmt.Printf("node%v go out append rpc\n",server)
			// 如果term变了，表示该结点不再是leader，什么也不做
			
			if rf.currentTerm != args.Term {
				// fmt.Printf("node %v change to follower when append entries\n",rf.me)
				rf.state=FOLLOWER
				return
			}
			//发现更大的term，本结点是旧leader
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				// fmt.Printf("node %v change to follower when append entries\n",rf.me)
				// rf.persist()
				return
			}

			if reply.Success {
				
				// fmt.Printf("node%v in...\n",server)
				
				rf.nextIndex[server] +=len(args.Entries)
				// fmt.Printf("nextIndex:%v\n",rf.nextIndex[server])
				// rf.matchIndex[server] = rf.nextIndex[server]-1
				// // 提交到哪个位置需要根据中位数来判断，中位数表示过半提交的日志位置，
				// // 每次提交日志向各结点发送的日志并不完全一样，不能光靠是否发送成功来判断
				// matchIndexSlice := make([]int, len(rf.peers))
				// for index, matchIndex := range rf.matchIndex {
				// 	matchIndexSlice[index] = matchIndex
				// }
				// copy(matchIndexSlice,rf.matchIndex)
				// sort.Slice(matchIndexSlice, func(i, j int) bool {
				// 	return matchIndexSlice[i] < matchIndexSlice[j]
				// })
			
			
				// newCommitIndex := matchIndexSlice[len(rf.peers)/2]
				newCommitIndex:=rf.nextIndex[server]-1
				// fmt.Printf("newcommitIndex:%v,rf.commitIndex:%v\n",newCommitIndex,rf.commitIndex)
				
				//不能提交不属于当前term的日志
				if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
					// fmt.Printf("start apply:%v\n",server)
					rf.commitIndex=newCommitIndex
					rf.applyLogs()
				}
				// fmt.Println(rf.commitIndex)
				
				
			} else if !reply.Success{
				// fmt.Printf("node %v' append entries failed, reply: %+v\n", server, *reply)
				switch reply.AppendError{
				case PreLogIndexError:
					//
					// fmt.Println("there1")
				case TermNotMatch:
					rf.nextIndex[server]=reply.ConflictIndex
					fmt.Println(reply.ConflictIndex)
					// fmt.Println("there2")
				case IndexNotExist:
					rf.nextIndex[server]=reply.ConflictIndex
					// fmt.Println("there3")

				}
			}
			
		}(i)
	}
}
	 

func (rf *Raft) getIndexByTerm(term int) int{
	i, j := 0, len(rf.log)-1
	for i < j {
		mid := (i + j) / 2
		if rf.log[mid].Term > term {
			j = mid
		} else if rf.log[mid].Term < term{
			i = mid + 1
		}else{
			return mid
		}
	}
	return 0
}
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// 重置选举计时器
func (rf *Raft) resetElectionTimer() {
	// fmt.Printf("node%v reset ElectionTimer\n",rf.me)
	timeout := ElectionTimeoutMin + time.Duration(rand.Intn(int(ElectionTimeoutMax-ElectionTimeoutMin)))
    rf.electionTimer = time.Now().Add(timeout)
}
func (rf *Raft) resetHeartbeatTimer() {
	// fmt.Printf("node%v resetHeartbeatTimer\n",rf.me)
	rf.heartbeatTimer = time.Now().Add(HeartbeatInterval)
}

// 服务或测试器希望创建一个 Raft 服务器。所有 Raft 服务器的端口（包括这个服务器的端口）都在 peers[] 中。
// 这个服务器的端口是 peers[me]。所有服务器的 peers[] 数组的顺序都是一样的。
// persister 是这个服务器用来保存其持久状态的地方，并且最初还保存着最近的持久化状态（如果有的话）。
// applyCh 是一个通道，测试器或服务期望 Raft 在该通道上发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应该启动 goroutines 来处理任何长时间运行的工作。
func Make(peers []*labrpc.ClientEnd, me int,persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.dead=0
	rf.state=FOLLOWER
	rf.currentTerm=0
	rf.votedFor=-1
	
	rf.log=make([]Entry,1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i:=0;i<len(peers);i++{
		rf.nextIndex[i]=1
	}
	rf.matchIndex = make([]int, len(peers))
	
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// fmt.Printf("create a raft node %v\n",rf.me)
	go rf.ticker()
	go rf.applyLogs()
	return rf
}


