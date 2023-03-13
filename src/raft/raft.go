package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// the types of server current identity
const (
	Inf int = 100000000

	Leader    int = 0
	Candidate int = 1
	Follower  int = 2

	electionTimeBase  int64 = 800
	electionTimeRange int64 = 400
	heartbeatInterval int64 = 80
	heartbeatTimeout  int64 = 400
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Entry struct {
	Term    int         // when the entry was received by leader
	Command interface{} // the command for state machine
}

// for the commitment of leader specially
// need lock to sync
type CountToCommit struct {
	count     int
	minCommit int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// persistent state
	identity    int     // server current identity
	currentTerm int     // latest term server has seen
	votedFor    int     // candidateId that received vote in current term
	log         []Entry // log entries

	timeRound *TimeControl

	// Volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leader
	// set all nextIndex 1 and all match index 0 for which is not a leader

	// for each server,index of the next log entry to send to that server
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	matchIndex []int

	voteGrant []bool // if the follower support it
}

// Use to coordinate election timeout and heartbeat arrival time
// if lastRcHBTime > lastRoundTime
// we can use electionTimeout - (lastRcHBTime - lastRoundTime) to
// compute the sleep time next Round
type TimeControl struct {
	mutex sync.Mutex // lock use to sync

	electionTimeout int64 // the timeout of a election
	lastRoundTime   int64 // the start time of the last round of election timeout
	lastRcHBTime    int64 // the last time that receive a heartbeat
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.identity == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//------------------------------------------//
// --------------Heartbeat------------------//
// -----------------------------------------//

// Invoked by leader to replicate log entries,also use as heartbeat
type AppendEntriesRPC struct {
	Term     int // leader's term
	LeaderId int // so followers can redirect clients

	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // term of prevLogIndex entry(empty for heartbeat)

	LeaderCommit int // leader's commitIndex
}

// the reply from followers
type AppendEntriesRPY struct {
	Term int // currentTerm, for leader to update itself

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

// send heartbeat to followers, only invoked by Leader
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesRPC, reply *AppendEntriesRPY) bool {
	// Debug(dLeader, "S%d: send heartbeat, with len(entries) %d", rf.me, len(args.Entries))

	ok := rf.peers[server].Call("Raft.HeartbeatResponse", args, reply)

	return ok
}

// when it become a leader, it start to do the func below
func (rf *Raft) LeaderAction() {

	for !rf.killed() {
		rf.mu.Lock()

		// if no longer a leader, end up it
		if rf.identity != Leader {
			rf.mu.Unlock()
			return
		}

		countToCommit := CountToCommit{
			count:     0,
			minCommit: Inf,
		}

		// Send heartbeat information regularly
		for i := 0; i < len(rf.peers); i++ {

			args := AppendEntriesRPC{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      make([]Entry, 0),
				LeaderCommit: rf.commitIndex,
			}

			// check if need to send log entries
			rf.DealEntries(i, &args)

			// deal with the appendies and reply
			go rf.DealAppendies(i, &args, &countToCommit)
		}

		rf.mu.Unlock()

		time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)
	}

}

// before every heartbeat is sent to server
// check if need to send log entries
func (rf *Raft) DealEntries(serverTo int, args *AppendEntriesRPC) {
	// leader.lastApplied >= the follower.nextIndex
	// indicates that the server has not received this log
	// append with log entries starting at nextIndex
	if rf.lastApplied >= rf.nextIndex[serverTo] {

		for i := rf.nextIndex[serverTo]; i <= rf.lastApplied; i++ {
			args.Entries = append(args.Entries, rf.log[i])
		}

		// the last one immediately preceding new ones
		args.PrevLogIndex = rf.nextIndex[serverTo] - 1
		args.PrevLogTerm = rf.log[rf.nextIndex[serverTo]-1].Term
	}
}

// send the appendEntries
// and deal after receiving the reply
func (rf *Raft) DealAppendies(serverTo int, args *AppendEntriesRPC, countToCommit *CountToCommit) {

	// set timeout for this rpc
	// timeout := time.Duration(heartbeatTimeout * int64(time.Millisecond))
	// done := make(chan bool, 1)

	reply := AppendEntriesRPY{}

	if !rf.SendAppendEntries(serverTo, args, &reply) {
		Debug(dError, "S%d: sending appendEntries", rf.me)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if still a leader
	if rf.identity == Leader {

		// error in rpc or receive from itself
		if reply.Term == 0 {
			return
		}

		// indicate a new election have started
		if reply.Term > args.Term {
			Debug(dTerm, "S%d: no longer a leader", rf.me)
			rf.ConvertToFollower(reply.Term)
			return
		}

		// the follower synchronized successfully
		if reply.Success && len(args.Entries) != 0 {
			rf.nextIndex[serverTo] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[serverTo] = rf.nextIndex[serverTo] - 1

			// since there hold the rf.mu,
			// the atomicity of the countToCommit operation can be guaranteed
			countToCommit.count++
			countToCommit.minCommit = Min(countToCommit.minCommit, rf.matchIndex[serverTo])

			if countToCommit.count > len(rf.peers)/2 && countToCommit.minCommit > rf.commitIndex &&
				rf.log[countToCommit.minCommit].Term == rf.currentTerm {

				// Debug(dCommit, "S%d: commit logs, index %d", rf.me, countToCommit.minCommit)
				go rf.Applier(rf.commitIndex+1, countToCommit.minCommit)

				rf.commitIndex = countToCommit.minCommit
			}

		} else if reply.Success && len(args.Entries) == 0 {
			return
		} else {
			// false
			// decrease the follower's nextIndex and retry

			rf.nextIndex[serverTo]--

			// todo
			// retry instantly or wait for the next loop?
		}
	}

}

// followers response to the heartbeat msg
func (rf *Raft) HeartbeatResponse(args *AppendEntriesRPC, reply *AppendEntriesRPY) {

	// update its last received times
	rf.timeRound.mutex.Lock()
	rf.timeRound.lastRcHBTime = time.Now().UnixMilli()
	rf.timeRound.mutex.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dLog2, "S%d: heartbeatresponse", rf.me)

	reply.Term = rf.currentTerm
	reply.Success = true

	// leader.currentTerm < follower.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// if itself, return true
	if args.LeaderId == rf.me {
		reply.Success = true
		return
	}

	// indicates the log entries before are out of sync
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// update the commitIndex if need
	if args.LeaderCommit > rf.commitIndex {
		tmp := Min(args.LeaderCommit, rf.lastApplied)

		// Debug(dCommit, "S%d: commit logs", rf.me)

		go rf.Applier(rf.commitIndex+1, tmp)

		rf.commitIndex = tmp
	}

	if len(args.Entries) > 0 {
		// add the new log entries into its log entries
		// if an existing entry conflicts with a new one, delete the existing entry
		for i := 1; i <= len(args.Entries); i++ {
			// rf.log[args.PrevLogIndex+i] = args.Entries[i]

			// todo
			if args.PrevLogIndex+i < len(rf.log) {
				rf.log[args.PrevLogIndex+i] = args.Entries[i-1]
			} else {
				rf.log = append(rf.log, args.Entries[i-1])
			}
		}
		rf.lastApplied = args.PrevLogIndex + len(args.Entries)
	}
}

// invoke when a server's commitIndex update
func (rf *Raft) Applier(begin, end int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if begin > end {
		return
	}

	for i := begin; i <= end; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}
}

//------------------------------------------//
// ---------------Election------------------//
// -----------------------------------------//

// example RequestVote RPC arguments structure.
// Invoked by candidates to gather votes
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dVote, "S%d: receive a request for vote", rf.me)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if rf.currentTerm < args.Term,
	// indicates that it will come to a new term, so it changes its state
	if rf.currentTerm < args.Term {
		rf.ConvertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// todo
	// delete rf.identity != Candidate?
	reply.VoteGranted = false
	// rf.votedFor == args.CandidateId prevent that the candidate send twice
	// the leader's LastLogTerm need to greater than the follower's lastLogTerm
	// if equals, compare the index
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[rf.lastApplied].Term ||
			(args.LastLogTerm == rf.log[rf.lastApplied].Term && args.LastLogIndex >= rf.lastApplied)) {

		reply.VoteGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// change the server's voteGrant
	rf.voteGrant[server] = reply.VoteGranted

	return ok
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

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
	// DPrintf("Server %d has been killed", rf.me)
	Debug(dInfo, "S%d: Been killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	CurrentTimeout := electionTimeBase + rand.Int63n(electionTimeRange)
	sleepTime := time.Duration(CurrentTimeout)

	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using

		// get the current time as the start time of this round
		// only used here, no need to lock
		rf.timeRound.lastRoundTime = time.Now().UnixMilli()

		// sleep loop
		time.Sleep(sleepTime * time.Millisecond)

		// if not a leader and timeout, start to election
		rf.mu.Lock()
		rf.timeRound.mutex.Lock()
		if rf.identity != Leader && rf.timeRound.lastRcHBTime <= rf.timeRound.lastRoundTime {
			Debug(dTimer, "S%d: timeout", rf.me)
			go rf.LeaderElection()
		}
		rf.timeRound.mutex.Unlock()
		rf.mu.Unlock()

		// prepare for next loop
		// set the timeout interval of election
		rand.Seed(time.Now().UnixMilli())
		rf.timeRound.electionTimeout = electionTimeBase + rand.Int63n(electionTimeRange)

		// get the mutex lock
		rf.timeRound.mutex.Lock()
		// if lastRcHBTime > lastRoundTime
		// we can use electionTimeout - (lastRoundTime + CurrentTimeout - lastRcHBTime) to
		// compute the sleep time next Round
		if rf.timeRound.lastRcHBTime > rf.timeRound.lastRoundTime {
			sleepTime = time.Duration(rf.timeRound.electionTimeout -
				(rf.timeRound.lastRoundTime + CurrentTimeout - rf.timeRound.lastRcHBTime))
		} else {
			sleepTime = time.Duration(rf.timeRound.electionTimeout)
		}
		rf.timeRound.mutex.Unlock()

		// update last round's timeout
		CurrentTimeout = rf.timeRound.electionTimeout
	}
}

// Vote for candidates inorder to confirm a leader
func (rf *Raft) LeaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ConvertToCandidate()

	// DPrintf("Server %d start a election", rf.me)
	Debug(dVote, "S%d: start a election in term %d", rf.me, rf.currentTerm)

	voteCount := 0 // it vote for itself, so init 1

	// Debug(dLeader, "S%d: lastApplied %d, loglen %d", rf.me, rf.lastApplied, len(rf.log))

	// send request to others for vote
	for i := 0; i < len(rf.peers); i++ {
		go func(serverTo int, currentTerm int, candidateId int,
			lastLogIndex int, lastLogTerm int) {

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			if !rf.sendRequestVote(serverTo, &args, &reply) {
				// Debug(dError, "S%d: Sending a request", rf.me)
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.ConvertToFollower(reply.Term)

			} else if reply.VoteGranted && reply.Term == rf.currentTerm {
				// Debug(dLeader, "S%d: get a vote", rf.me)

				voteCount++ // need to lock too

				// DPrintf("Server %d: receive a vote, now the vote is %d", rf.me, voteCount)

				// if more than half of the voteCounts
				// and it is still a candidate not follower, it convert to a leader
				if voteCount > len(rf.peers)/2 && rf.identity == Candidate {
					rf.ConvertToLeader()
					// DPrintf("Server %d become the leader", rf.me)
					Debug(dVote, "S%d: become the leader", rf.me)
				}
			}
			rf.mu.Unlock()

		}(i, rf.currentTerm, rf.me, rf.lastApplied, rf.log[rf.lastApplied].Term)
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// client -> leader
	if rf.identity == Leader {
		entry := Entry{
			Term:    rf.currentTerm,
			Command: command,
		}

		// add it into its log
		rf.log = append(rf.log, entry)

		// Debug(dClient, "S%d: leader add a log", rf.me)
		// increase the index of lastApplied
		rf.lastApplied++

		index = rf.lastApplied
	}

	isLeader = rf.identity == Leader
	term = rf.currentTerm

	return index, term, isLeader
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// todo
	DebugInit()

	rf.identity = Follower // initial state
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 indicate that it have not voted for anyone

	rf.timeRound = &TimeControl{
		lastRcHBTime: -1,
	}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Entry, 0)
	// simplify the implement, first insert an unuseful entry
	rf.log = append(rf.log, Entry{Term: 0, Command: nil})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.voteGrant = make([]bool, len(peers))

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}

	// DPrintf("Server %d start successfully", rf.me)
	Debug(dInfo, "S%d: start successfully", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) ConvertToFollower(term int) {
	// Debug(dLog, "S%d: convert to a follower", rf.me)

	rf.identity = Follower
	rf.currentTerm = term

	// reset
	rf.votedFor = -1
}

// only invoked in election stage
func (rf *Raft) ConvertToCandidate() {
	// start a new term
	rf.currentTerm++
	rf.identity = Candidate
	rf.votedFor = rf.me

	// delete?
	for i := range rf.voteGrant {
		rf.voteGrant[i] = false
	}
}

func (rf *Raft) ConvertToLeader() {
	rf.identity = Leader

	// update the matchIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	// start to perform tasks
	go rf.LeaderAction()
}
