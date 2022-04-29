package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)创建raft服务器
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)开始对一个新的log entry 进行共识
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)询问raft目前的term，以及是否为leader
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//   每次一个entry被提交到log中，每个raft 伙计都应该给service发个ApplyMsg（位于同一个server)

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

var rw sync.RWMutex

// ApplyMsg
// raft peer得知 log entries 已经提交的时候，需要在同一个服务器上发送ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
// go对象来实现一个raft对象
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 保护对peer状态的共享访问
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]   在peers数组中的index
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state       int // 0-follower, 1-candidate, 2-leader
	currentTerm int // 当前任期
	votedFor    int // 当前term内，收到投票的候选者id

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// UpdateTerm
// 更新raft peer所处的term
func (rf *Raft) UpdateTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.mu.Unlock()
}

// ChangeState
// 更新状态
func (rf *Raft) ChangeState(state int) {
	rf.mu.Lock()
	rf.state = state
	rf.mu.Unlock()
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
// 返回当前term 和 server是否相信他就是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	switch rf.state {
	// obtain state of raft
	case 0:
		isleader = false
	case 1:
		isleader = false
	case 2:
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 将raft 持久化状态保存到稳定的存储中，在宕机以后恢复的依据，参考图2要持久化哪些东西
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// readPersist
// restore previously persisted state.
// 恢复持久化状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 根据快照恢复
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 参数名称必须以大写字符开头！
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int // 候选人所处term
	CandidateId   int // 候选人id
	LastLogIndex  int // 最后一个log entry的编号
	LastLogTerm   int // 最后一个log entry的term
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	FollowerTerm int  // 候选人来更新自己
	VoteGranted  bool // 候选人是否获得投票
}

type AppendEntriesArgs struct {
	LeaderTerm   int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry  immediately preceding new ones
	entries      []int // log entries to store
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	term    int  // currentTerm, for leader to update itself
	success bool // true if follower
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

func (rf *Raft) sendAppendEntries() {

}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果candidate的term < currentTerm 则不投票
	if rf.killed() { // 如果server is dead，别管他了
		return
	}
	rw.Lock()
	defer rw.Unlock()
	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.FollowerTerm = rf.currentTerm
	} else {
		if rf.currentTerm == args.CandidateTerm { // 任期相同
			if rf.votedFor == -1 { // 未投票
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else { // 已投票，不再投票
				reply.VoteGranted = false
			}
			reply.FollowerTerm = rf.currentTerm
		} else { // 任期超前
			rf.state = 0 // 无论之前什么state，回到follower
			rf.currentTerm = args.CandidateTerm
			rf.votedFor = args.CandidateId

			reply.VoteGranted = true
			reply.FollowerTerm = rf.currentTerm
		}
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 如果peer没有最近没有收到心跳包，那么ticker开始一轮新的选举
func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.state {
		case 0: // follower 状态

		case 1: // candidate 状态

		case 2: // leader状态
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep(). 需要确定是否应该开启选举，然后随机睡眠时间

	}
}

// Make
// 必须立马返回
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// TODO Your initialization code here (2A, 2B, 2C).
	rf.state = 0
	rf.currentTerm = 0 // FIXME 初始化为0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
