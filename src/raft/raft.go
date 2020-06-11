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
	electionTimeOutMax     = 3000 * time.Millisecond
	electionTimeOutMin     = 1000 * time.Millisecond
	heartBeatLoopItv       = 100 * time.Millisecond
	checkLastLogIdxLoopItv = 10 * time.Millisecond
	appendEntriesRetryItv  = 10 * time.Millisecond
	updateCommitIdxLoopItv = 10 * time.Millisecond

	followerState  State = "Follower"
	candidateState State = "Candidate"
	leaderState    State = "Leader"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type State string

// import "bytes"
// import "../labgob"

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
	applyCh        chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v (%v) get a Start, get lock, command is %+v", rf.me, rf.state, command)
	if rf.killed() {
		isLeader = false
		DPrintf("%v (%v) return to Start() is index: %v, term: %v, it is killed", rf.me, rf.state, index, term)
		return index, term, isLeader
	}
	isLeader = rf.state == leaderState
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		index = len(rf.log) - 1
		term = rf.currentTerm
	}
	DPrintf("%v (%v) return to Start() is index: %v, term: %v", rf.me, rf.state, index, term)
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
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		resetTimerChan: make(chan int, 100),
		state:          followerState,
		applyCh:        applyCh,
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.notLeaderCond = sync.NewCond(&rf.mu)
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// kick off election timeout
	go rf.handleElectionTimeOutLoop()
	// kick off check last login index
	go rf.checkLastLogIdxWithFollowerLoop()
	// kick off updating commit index
	go rf.updateCommitIdxLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) handleElectionTimeOutLoop() {
outer:
	for {
		rf.notLeaderCond.L.Lock()
		if rf.killed() {
			DPrintf("%v (%v) is killed", rf.me, rf.state)
			rf.notLeaderCond.L.Unlock()
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

func (rf *Raft) handleHeartBeatLoop() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			DPrintf("%v (%v) is killed", rf.me, rf.state)
			rf.mu.Unlock()
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
		time.Sleep(heartBeatLoopItv)
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
	if rf.killed() {
		DPrintf("%v (%v) is killed", rf.me, rf.state)
		return
	}
	if rf.state == candidateState {
		DPrintf("%v (%v) is starting an election, get lock", rf.me, rf.state)
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
			// send and handle RequestVote RPC
			go func() {
				rf.sendRequestVote(idx, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("%v (%v, term: %v) RequestVote RPC call to %v success, result is %v", rf.me, rf.state, rf.currentTerm, idx, reply)
				// convert to follower if currentTerm lags behind
				if reply.Term > rf.currentTerm {
					DPrintf("%v (%v) get return with currentTerm lagging behind!", rf.me, rf.state)
					rf.convertToFollower(reply.Term)
					return
				}
				// count votes
				if rf.state == candidateState && reply.VoteGranted {
					DPrintf("%v (%v) get vote from %v!", rf.me, rf.state, idx)
					cnt++
					if cnt == len(rf.peers)/2+1 {
						// convert to leader
						rf.state = leaderState
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}

						DPrintf("%v (%v) get majority voting! cnt=%v, log=%+v", rf.me, rf.state, cnt, rf.log)
						// upon election: send initial empty AppendEntries RPCs
						go rf.handleHeartBeatLoop()
					}
				}
			}()
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		idx := i
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		DPrintf("%v (%v) ready to send heartbeats!", rf.me, rf.state)
		// send heartbeat AppendEntries RPC and handle it
		go func() {
			ok := rf.sendAppendEntries(idx, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				DPrintf("%v (%v, Term: %v) failed to send heartbeats!", rf.me, rf.state, rf.currentTerm)
				return
			}
			DPrintf("%v (%v) heartbeats: sendAppendEntries status: %v", rf.me, rf.state, ok)
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

func (rf *Raft) checkLastLogIdxWithFollowerLoop() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			return
		}
		if rf.state != leaderState {
			rf.mu.Unlock()
			time.Sleep(checkLastLogIdxLoopItv)
			continue
		}
		DPrintf("%v (%v, Term: %v) has log %+v", rf.me, rf.state, rf.currentTerm, rf.log)
		DPrintf("%v (%v) start checking last log index with followers", rf.me, rf.state)
		for i := range rf.peers {
			idx := i
			if i == rf.me {
				continue
			}
			if len(rf.log)-1 >= rf.nextIndex[i] {
				DPrintf("%v (%v) send non-heartbeat AppendEntries RPC to %v", rf.me, rf.state, idx)
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					Entries:      make([]LogEntry, len(rf.log[rf.nextIndex[i]:])),
					LeaderCommit: rf.commitIndex,
				}
				copy(args.Entries, rf.log[rf.nextIndex[i]:])
				reply := &AppendEntriesReply{}
				// send non-heartbeat AppendEntries and handle it
				go func() {
					// retry sending AppendEntries until succeeds
					for {
						rf.mu.Lock()
						if rf.state != leaderState {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						ok := rf.sendAppendEntries(idx, args, reply)
						rf.mu.Lock()
						if !ok {
							DPrintf("%v (%v, Term: %v) failed to send non-heartbeat AppendEntries RPC to %v", rf.me, rf.state, rf.currentTerm, idx)
							rf.mu.Unlock()
							return
						}
						if reply.Term > rf.currentTerm {
							rf.convertToFollower(reply.Term)
							rf.mu.Unlock()
							return
						}
						if reply.Term > args.Term {
							rf.mu.Unlock()
							return
						}
						if reply.Success {
							break
						} else {
							DPrintf("%v (%v) retry sending non-heartbeat AppendEntries RPC to %v, prev args: %+v", rf.me, rf.state, idx, args)
							// AppendEntries RPC fails
							// reduce nextIndex, modify args
							prevLogTerm := args.PrevLogTerm
							for rf.log[args.PrevLogIndex].Term == prevLogTerm {
								args.PrevLogIndex -= 1
							}
							args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
							args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
							copy(args.Entries, rf.log[args.PrevLogIndex+1:])
							rf.mu.Unlock()
							time.Sleep(appendEntriesRetryItv)
						}
					}
					DPrintf("%v (%v) successfully send non-heartbeat AppendEntries RPC to %v, args: %+v", rf.me, rf.state, idx, args)
					// AppendEntries RPC succeeds
					rf.nextIndex[idx] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[idx])
					rf.matchIndex[idx] = rf.nextIndex[idx] - 1
					DPrintf("%v (%v) append non-heartbeat AppendEntries Success, rf.nextIndex[idx] = %v", rf.me, rf.state, rf.nextIndex[idx])
					rf.mu.Unlock()
				}()
			}
		}
		rf.mu.Unlock()
		time.Sleep(checkLastLogIdxLoopItv)
	}
}

func (rf *Raft) updateCommitIdxLoop() {
	// update rf.commitIndex
	for {
		rf.mu.Lock()
		if rf.state == leaderState {
			possible := true
			for newCommitIdx := rf.commitIndex + 1; possible; {
				possible = false
				cnt := 1
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= newCommitIdx {
						possible = true
					}
					if rf.matchIndex[i] >= newCommitIdx && rf.log[newCommitIdx].Term == rf.currentTerm {
						cnt++
					}
				}
				if cnt > len(rf.peers)/2 {
					DPrintf("%v (%v) sending to applyCh, cmd: %v, cmdIdx: %v", rf.me, rf.state, rf.log[newCommitIdx].Command, newCommitIdx)
					for t := rf.commitIndex + 1; t <= newCommitIdx; t++ {
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.log[t].Command,
							CommandIndex: t,
						}
					}

					rf.commitIndex = newCommitIdx
				}
				newCommitIdx++
			}
		}
		rf.mu.Unlock()
		time.Sleep(updateCommitIdxLoopItv)
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

func min(a int, b int) (c int) {
	c = a
	if b < a {
		c = b
	}
	return
}

func max(a int, b int) (c int) {
	c = a
	if b > a {
		c = b
	}
	return
}
