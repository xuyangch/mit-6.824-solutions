package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

const (
	electionTimeOutMax       = 2500 * time.Millisecond
	electionTimeOutMin       = 1450 * time.Millisecond
	heartBeatInterval        = 100 * time.Millisecond
	followerState      State = "Follower"
	candidateState     State = "Candidate"
	leaderState        State = "Leader"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type State string

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile state for all
	commitIndex int
	// volatile state for leaders
	nextIndex  []int
	matchIndex []int

	// others
	resetTimerChan chan int
	state          State
	notLeaderCond  *sync.Cond
}

type LogEntry struct {
	Term    int
	Command string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == leaderState
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// RequestVote RPC Call.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v (%v) receive RequestVote request, get lock", rf.me, rf.state)
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if rf.state == followerState {
		if args.Term < rf.currentTerm {
			return
		}
		// if grand vote, reset timer
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateUpToDate(args.Term, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.resetTimerChan <- 1
		}
	}
}

// AppendEntries RPC Call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v, (%v) Received AppendEntries, get lock", rf.me, rf.state)
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Success = false
	reply.Term = rf.currentTerm
	if rf.state == followerState || rf.state == candidateState {
		if args.Term < rf.currentTerm {
			return
		}

		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}

		if rf.state == followerState {
			DPrintf("%v, (%v) reset timeout", rf.me, rf.state)
			rf.resetTimerChan <- 1
		}
		if rf.state == candidateState {
			rf.convertToFollower(args.Term)
		}
		// TODO: 3. If an existing entry conflicts with a new one (same index
		//		but different terms), delete the existing entry and all that
		//		follow it (§5.3)
		// TODO: 4. Append any new entries not already in the log
		// TODO: 5. If leaderCommit > commitIndex, set commitIndex =
		//			min(leaderCommit, index of last new entry)
	}
}

// check if candidate is at least up-to-date with rf
func (rf *Raft) isCandidateUpToDate(candidateTerm int, candidateLastLogIdx int) bool {
	rfLastLogIdx := len(rf.log) - 1
	if candidateTerm != rf.log[rfLastLogIdx].Term {
		return candidateTerm > rf.log[rfLastLogIdx].Term
	}
	return candidateLastLogIdx >= rfLastLogIdx
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]LogEntry, 1),
		commitIndex:    0,
		nextIndex:      nil,
		matchIndex:     nil,
		resetTimerChan: make(chan int, 100),
		state:          followerState,
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.notLeaderCond = sync.NewCond(&rf.mu)

	// kick off election timeout
	go handleElectionTimeOut(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func handleElectionTimeOut(rf *Raft) {
outer:
	for {
		rf.notLeaderCond.L.Lock()
		if rf.killed() {
			DPrintf("%v (%v) is killed", rf.me, rf.state)
			return
		}
		// if leaderState, no need to wait for time out, wait for state downgrade
		for rf.state == leaderState {
			rf.notLeaderCond.Wait()
		}
		rf.notLeaderCond.L.Unlock()
		select {
		case <-rf.resetTimerChan:
			continue
		case <-time.After(time.Duration(int64(electionTimeOutMin) + rand.Int63n(int64(electionTimeOutMax-electionTimeOutMin)))):
			// timeout, start voting
			rf.mu.Lock()
			if rf.state == leaderState {
				rf.mu.Unlock()
				continue outer
			}
			DPrintf("%v (%v) timeout, start new election", rf.me, rf.state)
			// follower timeout
			if rf.state == followerState {
				// convert to candidate, start new election below
				rf.state = candidateState
				rf.notLeaderCond.Signal()
			}
			rf.startElection()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) handleHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			DPrintf("%v (%v) is killed", rf.me, rf.state)
			return
		}
		if rf.state != leaderState {
			rf.mu.Unlock()
			return
		}
		DPrintf("%v (%v) start sending heartbeat!", rf.me, rf.state)
		rf.sendHeartBeats()
		DPrintf("%v (%v) sending heartbeat to all success!", rf.me, rf.state)
		rf.mu.Unlock()
		time.Sleep(heartBeatInterval)
	}
}

func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = followerState
	//reset votedFor
	rf.votedFor = -1
	rf.notLeaderCond.Signal()
	DPrintf("%v (%v) Converted to Follower", rf.me, rf.state)
}

func (rf *Raft) startElection() {
	if rf.state == candidateState {
		// start new election
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetTimerChan <- 1

		DPrintf("%v (%v) sending RequestVote RPC to all peers", rf.me, rf.state)

		cnt := 1
		// send RequestVote RPC to all peers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			idx := i
			// prepare RPC
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := &RequestVoteReply{}
			DPrintf("%v (%v) sending RequestVote RPC to %v ...", rf.me, rf.state, i)
			go func() {
				rf.sendRequestVote(idx, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("%v (%v, term: %v) RequestVote RPC call to %v success, result is %v", rf.me, rf.state, rf.currentTerm, i, reply)
				// convert to follower if currentTerm lags behind
				if reply.Term > rf.currentTerm {
					DPrintf("%v (%v) get return with currentTerm lagging behind!", rf.me, rf.state)
					rf.convertToFollower(reply.Term)
					return
				}
				// add votes
				if reply.VoteGranted {
					DPrintf("%v (%v) get vote from %v!", rf.me, rf.state, idx)
					cnt++
					if cnt == len(rf.peers)/2+1 {
						rf.state = leaderState
						DPrintf("%v (%v) get majority voting! cnt=%v", rf.me, rf.state, cnt)
						// upon election: send initial empty AppendEntries RPCs
						go rf.handleHeartBeat()
					}
				}
			}()
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	for i := range rf.peers {
		idx := i
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.log) - 1,
			PrevLogTerm:  rf.log[len(rf.log)-1].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		DPrintf("%v (%v) ready to send!", rf.me, rf.state)
		if i == rf.me {
			continue
		}
		go func() {
			ok := rf.sendAppendEntries(idx, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("%v (%v) sendAppendEntries status: %v", rf.me, rf.state, ok)
			if rf.killed() {
				DPrintf("%v (%v) is killed", rf.me, rf.state)
				return
			}
			if reply.Term > rf.currentTerm {
				DPrintf("%v (%v) get AppendEntry reply with currentTerm lagging behind!", rf.me, rf.state)
				rf.convertToFollower(reply.Term)

				return
			}
		}()
	}
}
