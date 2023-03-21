package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log LogEntry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new LogEntry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const Follower = 0
const Candidate = 1
const Leader = 2

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry (§5.4)
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here .
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store, empty for heartbeat
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	ConflictTerm  int // 1st inconsistent term
	ConflictIndex int // 1st inconsistent term's index
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// abstract message emitter
func sendMessage(ch chan bool) {
	go func() {
		select {
		case <-ch:
		default:
		}
		ch <- true
	}()
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 论文给出的字段：Persistent state on all servers
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries

	// 论文给出的字段：Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// 论文给出的字段：Volatile state on leaders, Reinitialized after election
	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on

	// 自己添加的字段
	currentState      int           // server的当前状态: Follower Candidate Leader
	heartbeatInterval time.Duration //心跳的间隔

	// 自己添加的字段：用于通信的信道
	applyCh       chan ApplyMsg // 发送Apply Message
	grantVoteCh   chan bool     // Candidate的投票
	appendEntryCh chan bool     // leader的AppendEntry
	leaderCh      chan bool     // 竞选成功后向此通道发送信息，提示自己成为leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here .
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.currentState == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 下面两个函数用来分别存储、读取server上的持久性状态，包括：currentTerm、voteFor和log[]
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 {
		return
	}
}

//
// example RequestVoteHandler RPC handler.
//
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求投票的server的term落后，则返回自己的term并拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 此时自己的term <= 请求者的term。为什么先做这一步？因为若args.Term > rf.currentTerm，则自己一定是follower，需要先转换状态
	if args.Term > rf.currentTerm {
		rf.currentState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// 如果自己已经投过票，则拒绝投票
	if rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 只有当请求者的log至少和自己的log一样新时，才投票
	lastLogIndex := len(rf.log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	/*
	1. args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex(任期相同但index更新)
	2. args.LastLogTerm > lastLogTerm(任期更新)
	其他情况都拒绝投票
	*/
	if (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) || args.LastLogTerm > lastLogTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		sendMessage(rf.grantVoteCh)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// do what an AppendEntriesRPC receiver should do
	if args.Term < rf.currentTerm { // return false when received obsolete RPC
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 为什么要在判断arg的term是否过时后才send：如果自己的Term不新于args的，则自己一定是Follower
	sendMessage(rf.appendEntryCh)

	rf.currentTerm = args.Term
	rf.currentState = Follower
	rf.votedFor = args.LeaderId
	rf.persist()

	// 提出prevLogTerm
	prevLogTerm := 0
	if len(rf.log) >= args.PrevLogIndex { // viable
		if args.PrevLogIndex > 0 {
			prevLogTerm = rf.log[args.PrevLogIndex-1].Term // local prevLogTerm
		}
	}

	/*
	rf.log的长度小于prevLogIndex, 或者 rf.log[prevLogIndex-1]的term与prevLogTerm不符, 则返回false, 并记录下产生冲突的index及term
	*/
	// rf.log的长度小于prevLogIndex
	if len(rf.log) < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// rf.log在prevLogIndex的term与prevLogTerm不符
	if args.PrevLogTerm != prevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = prevLogTerm
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == prevLogTerm {
				reply.ConflictIndex = i + 1 //1st inconsistent term's index
				break
			}
		}
		return
	}
	/*
	args.PrevLogIndex==0, 或者 len(rf.log)>=args.PrevLogIndex && args.PrevLogTerm == prevLogTerm：成功，需要同步log，并apply
	*/
	// 此处必有 len(rf.log) >= args.PrevLogIndex
	if args.PrevLogIndex == 0 || args.PrevLogTerm == prevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = true

		rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...) //根据Figure2的描述，此处可以直接同步日志
		lastNewEntryIndex := len(rf.log)

		// receiver implementation 5
		if args.LeaderCommit > rf.commitIndex {
			tmp := math.Min(float64(args.LeaderCommit), float64(lastNewEntryIndex))
			rf.commitIndex = int(tmp)
		}
		rf.persist()
		rf.applyLogs()
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}

// finally: commit the logs
func (rf *Raft) applyLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		m := ApplyMsg{}
		m.Index = rf.lastApplied
		m.Command = rf.log[rf.lastApplied-1].Command
		rf.applyCh <- m
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.currentState == Leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		index = len(rf.log)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// candidate向其他server发送RequestVote RPC
func (rf *Raft) RequestVoteRPC() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 获取本server的最后一条log index以及log term
	lastLogIndex := len(rf.log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	// 要发送的消息
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}

	// 投票给自己的peer数，目前只有自己
	receivedVotes := 1

	// 下面开始选举
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := RequestVoteReply{}
			answer := rf.sendRequestVote(i, &args, &reply)
			if answer {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.currentState = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if rf.currentTerm != args.Term || rf.currentState != Candidate {
					return
				}

				if reply.VoteGranted {
					receivedVotes++
				}

				if receivedVotes > len(rf.peers)/2 {
					rf.currentState = Leader

					// 初始化leader所特有的两个volatile state
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}

					sendMessage(rf.leaderCh)
				}
			}
		}(i)
	}
}

func (rf *Raft) AppendEntriesRPCRun(id int) {
	for {
		rf.mu.Lock()
		if rf.currentState != Leader {
			rf.mu.Unlock()
			return
		}

		// 读取nextIndex数组，提取出需要发送给其他server的entries
		prevLogIndex := rf.nextIndex[id] - 1
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}
		entriesToAppend := append([]LogEntry{}, rf.log[rf.nextIndex[id]-1:]...)
		args := AppendEntriesArgs{
			rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entriesToAppend, rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		// 将组装好的消息发送给对应的server
		ret := rf.sendAppendEntries(id, &args, &reply)
		if ret {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				// term大于自己，变成follower
				rf.currentTerm = reply.Term
				rf.currentState = Follower
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			}

			if rf.currentState != Leader || rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				// 若成功接收，更新对应follower的nextIndex和matchIndex
				rf.matchIndex[id] = prevLogIndex + len(entriesToAppend)
				rf.nextIndex[id] = rf.matchIndex[id] + 1

				//  论文中图2的"Rules for Servers"中Leaders最后一条
				tmplist := make([]int, len(rf.peers))
				copy(tmplist, rf.matchIndex)
				tmplist[rf.me] = len(rf.log)
				sort.Ints(tmplist)
				N := tmplist[len(rf.peers)/2]
				if N > rf.commitIndex && rf.log[N-1].Term == rf.currentTerm {
					rf.commitIndex = N
				}
				rf.applyLogs()
				rf.mu.Unlock()
				return
			} else {
				// log的不一致性导致AppendEntries不成功，减小nextIndex并重试
				log_inconsistency := false
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == reply.ConflictTerm {
						log_inconsistency = true
					}
					if rf.log[i].Term > reply.ConflictTerm {
						if log_inconsistency {
							rf.nextIndex[id] = i
						} else {
							rf.nextIndex[id] = reply.ConflictIndex
						}
						break
					}
				}
				if rf.nextIndex[id] < 1 {
					rf.nextIndex[id] = 1
				}
				rf.mu.Unlock()
			}
		}
	}
}

// leader向别的server定时发送心跳或增加日志指令
func (rf *Raft) AppendEntriesRPC() {
	for {
		// 只有leader可以发
		rf.mu.Lock()
		if rf.currentState != Leader {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.AppendEntriesRPCRun(i)
			}
		}

		// time.Sleep(100 * time.Millisecond)
		time.Sleep(rf.heartbeatInterval)
	}
}

// raft服务器运行的主函数
func (rf *Raft) serverRun() {
	for {
		rf.mu.Lock()
		currentState := rf.currentState
		rf.mu.Unlock()

		// 每轮for循环都重设一次选举超时时长
		// electionTimeout := time.Duration(200+rand.Intn(200)) * time.Millisecond
		// 150+rand.Intn(350)
		electionTimeout := time.Duration(150+rand.Intn(350)) * time.Millisecond

		switch currentState {
		case Follower:
			select {
			case <-rf.appendEntryCh: //这部分逻辑由AppendEntriesHandler处理

			case <-rf.grantVoteCh: // reset timer

			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.currentState = Candidate //set to candidate again
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()
			}

		case Candidate:
			// instantly start election
			go rf.RequestVoteRPC()

			select {
			// 收到了leader发来的消息，变回follower
			case <-rf.appendEntryCh:
				rf.mu.Lock()
				// rf.currentTerm
				rf.currentState = Follower
				rf.votedFor = -1 // reset votedFor
				rf.persist()
				rf.mu.Unlock()

			// （自己）已经成为leader：reset timer
			case <-rf.leaderCh:

			case <-time.After(electionTimeout):
				rf.mu.Lock()
				if rf.currentState == Follower {
					rf.mu.Unlock()
					continue
				}
				rf.currentState = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()
			}

		case Leader:
			rf.AppendEntriesRPC()
			// time.Sleep(rf.heartbeatInterval)
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 1. 初始化一个raft服务器
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.currentState = Follower
	rf.heartbeatInterval = time.Duration(60) * time.Millisecond

	rf.applyCh = applyCh
	rf.grantVoteCh = make(chan bool)
	rf.appendEntryCh = make(chan bool)
	rf.leaderCh = make(chan bool)

	// 2. initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 3. 开始运行服务器：启动各个协程
	go rf.serverRun()

	// 4. 启动完毕，Make函数返回
	return rf
}
