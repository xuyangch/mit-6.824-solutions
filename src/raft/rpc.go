package raft

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
	//defer rf.persist()
	DPrintf("%v (%v) receive RequestVote request from %v, get lock, args is %+v, log is: %+v", rf.me, rf.state, args.CandidateId, args, rf.log)
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
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateUpToDate(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
			rf.resetTimerChan <- 1
		}
	}
}

// AppendEntries RPC Call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("%v (%v) Received AppendEntries from %v, trying to get lock", rf.me, rf.state, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	DPrintf("%v (%v) Received AppendEntries from %v, get lock", rf.me, rf.state, args.LeaderId)
	DPrintf("%v (%v) args.Term: %v, rf.currentTerm: %v, args: %+v", rf.me, rf.state, args.Term, rf.currentTerm, args)
	DPrintf("%v (%v Term: %v) has log (size: %v) %+v", rf.me, rf.state, rf.currentTerm, len(rf.log), rf.log)
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
			DPrintf("%v (%v) reset timeout", rf.me, rf.state)
			rf.resetTimerChan <- 1
		}
		if rf.state == candidateState {
			rf.convertToFollower(args.Term)
		}

		reply.Success = true

		// If an existing entry conflicts with a new one (same index but different terms),
		//		delete the existing entry and all that follow it
		DPrintf("%v (%v) Start matching logs", rf.me, rf.state)

		for eI := range args.Entries {
			localI := args.PrevLogIndex + eI + 1
			if (localI >= len(rf.log)) || (localI < len(rf.log) && rf.log[localI].Term != args.Entries[eI].Term) {
				if localI < len(rf.log) {
					rf.log = rf.log[:localI]
				}
				break
			}
		}

		// Append any new entries not already in the log
		modified := false
		for eI := range args.Entries {
			localI := args.PrevLogIndex + eI + 1
			if localI >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[eI])
				modified = true
			} else {
				rf.log[localI] = args.Entries[eI]
			}
		}

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		prevCommitIdx := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		}
		for i := prevCommitIdx + 1; i <= rf.commitIndex; i++ {
			DPrintf("%v (%v) sending to applyCh, cmd: %v, cmdIdx: %v", rf.me, rf.state, rf.log[i].Command, i)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		if modified {
			rf.persist()
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
