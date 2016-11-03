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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"strings"
	"sync"
	"time"
)

func randomTimeout() time.Duration {
	rand.Seed(time.Now().UTC().UnixNano())
	millis := 150 + rand.Intn(201)
	return time.Duration(millis) * time.Millisecond
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	t0        time.Time
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm  int
	VotedFor     int
	LastVoteTerm int
	Log          []LogEntry

	// Volatile
	CommitIndex      int
	LastApplied      int
	VotesReceived    int
	electionTimeout  *ElectionTimeout
	heartbeatsTicker *HeartbeatsTicker
	applyCh          chan ApplyMsg
	electionDisabled bool

	// Volatile data when leader
	NextIndex  []int
	MatchIndex []int

	State string
}

type LogEntry struct {
	Data interface{}
	Term int
}

type ElectionTimeout struct {
	stopChan    chan struct{}
	restartChan chan struct{}
}

type HeartbeatsTicker struct {
	ticker      *time.Ticker
	stopChan    chan struct{}
	restartChan chan struct{}
}

func (rf *Raft) restartElectionTimeout() {
	rf.electionTimeout.restartChan <- *new(struct{})
}

func (rf *Raft) restartHeartbeatsTicker() {
	rf.heartbeatsTicker.restartChan <- *new(struct{})
}

func (rf *Raft) startElectionTimeout(timeout time.Duration) {
	if rf.electionTimeout != nil {
		rf.restartElectionTimeout()
		return
	}

	rf.electionTimeout = &ElectionTimeout{}
	rf.electionTimeout.stopChan = make(chan struct{}, 100)
	rf.electionTimeout.restartChan = make(chan struct{}, 100)

	go func() {
		ticker := time.NewTicker(timeout)
		for {
			select {
			case <-rf.electionTimeout.stopChan:
				rf.log("\033[33m*** STOPPING ELECTION TIMEOUT ***{\033[0m\n")
				ticker.Stop()
				rf.log("}*** STOPPED ELECTION TIMEOUT ***\n")
			case <-rf.electionTimeout.restartChan:
				rf.log("\033[33m*** RESTARTING ELECTION TIMEOUT ***{\033[0m\n")
				ticker.Stop()
				ticker = time.NewTicker(timeout)
				rf.log("\033[33m}*** RESTARTING ELECTION TIMEOUT ***\033[0m\n")
			case <-ticker.C:
				rf.log("\033[33m*** ELECTION TIMEOUT TERM = %d ***{\033[0m\n", rf.CurrentTerm+1)
				rf.log("\033[33mElection timeout! Node %d starting new election\033[0m\n", rf.me)
				rf.startElection()
				rf.log("\033[33m}*** ELECTION TIMEOUT TERM = %d ***\033[0m\n", rf.CurrentTerm)
			}
		}
	}()

}

func (rf *Raft) stopElectionTimeout() {
	if rf.electionTimeout != nil {
		rf.log("Stopping election timeout\n")
		rf.electionTimeout.stopChan <- *new(struct{})
	} else {
		rf.log("Tried to stop electionTimeout, but none running\n")
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.CurrentTerm
	var isleader bool = rf.State == "leader"

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.me)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.LastVoteTerm)
	e.Encode(len(rf.Log))
	for i := range rf.Log {
		e.Encode(rf.Log[i].Data)
		e.Encode(rf.Log[i].Term)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.me)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.LastVoteTerm)
	var entries int
	d.Decode(&entries)
	rf.Log = make([]LogEntry, entries)
	for i := 0; i < entries; i += 1 {
		d.Decode(&rf.Log[i].Data)
		d.Decode(&rf.Log[i].Term)
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) lastEntry() LogEntry {
	if len(rf.Log) == 0 {
		return LogEntry{
			Term: -1,
			Data: 0,
		}
	}
	return rf.Log[len(rf.Log)-1]
}

func tabs(i int) string {
	return strings.Repeat("\t", i)
}

func (rf *Raft) noElections(disabled bool) {
	rf.electionDisabled = disabled
}

func (rf *Raft) VoteYes(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.log("%d -> RequestVoteReply -> %d: Vote\n", rf.me, args.CandidateId)
	rf.VotedFor = args.CandidateId
	rf.LastVoteTerm = args.Term
	reply.VoteGranted = true
	rf.CurrentTerm = args.Term
	rf.startElectionTimeout(randomTimeout())
}

func (rf *Raft) isAsUpToDate(lastLogIndexCandidate int, lastLogTermCandidate int) bool {
	if rf.lastEntry().Term == lastLogTermCandidate {
		return len(rf.Log) <= lastLogIndexCandidate
	} else {
		return rf.lastEntry().Term <= lastLogTermCandidate
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.noElections(false)
	rf.log("Received request vote from %d \n", args.CandidateId)
	rf.logCurrentState()
	rf.noElections(true)

	// the following definition of being up-to-date comes from the article's page 8, paragraph before 5.4.2
	candidateIsAsUp2Date := rf.isAsUpToDate(args.LastLogIndex, args.LastLogTerm)
	rf.log("%d <- RequestVoteRequest <- %d: IsUpToDate? %t\n", rf.me, args.CandidateId, candidateIsAsUp2Date)

	reply.Term = rf.CurrentTerm

	if rf.CurrentTerm > args.Term || rf.LastVoteTerm == args.Term || !candidateIsAsUp2Date {
		if args.Term > rf.CurrentTerm {
			rf.stopHeartbeats()
			if rf.State != "follower" {
				rf.startElectionTimeout(randomTimeout())
			}
			rf.CurrentTerm = args.Term
			rf.State = "follower"
		}
		// Log why the vote was denied
		if rf.CurrentTerm > args.Term {
			rf.log("%d -> RequestVoteReply -> %d: Term outdated\n", rf.me, args.CandidateId)
		} else if rf.LastVoteTerm == args.Term {
			rf.log("%d -> RequestVoteReply -> %d: Already Voted for term = %d \n", rf.me, args.CandidateId, rf.LastVoteTerm)
		} else {
			rf.log("%d -> RequestVoteReply -> %d: Candidate is not up to date\n", rf.me, args.CandidateId)
		}
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			rf.stopHeartbeats()
			rf.State = "follower"
		}
		rf.VoteYes(args, reply)
	}

}

func (rf *Raft) logCurrentState() {
	rf.log("[Node = %d, Term = %d, CommitIndex = %d, Log = %v, LastVoteTerm = %d ]\n", rf.me, rf.CurrentTerm, rf.CommitIndex, rf.Log, rf.LastVoteTerm)
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log("Received AppendEntries from %d: %v - PrevLogIndex = %d, PrevLogTerm = %d, LeaderCommit = %d\n", args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	rf.logCurrentState()

	reply.Term = rf.CurrentTerm

	if (rf.State == "candidate" && args.Term >= rf.CurrentTerm) || args.Term > rf.CurrentTerm {
		if rf.State == "leader" {
			rf.log("Stepping down!\n")
			rf.stopHeartbeats()
			rf.log("Theoretically stopped sending heartbeats\n")
		}
		rf.stopElectionTimeout()
		rf.log("%d <- AppendEntries <- %d: New Leader Detected\n", rf.me, args.LeaderId)
		rf.CurrentTerm = args.Term
		rf.State = "follower"
	}
	if rf.State == "follower" && args.Term >= rf.CurrentTerm {
		rf.startElectionTimeout(randomTimeout())
		reply.Success = rf.appendToLog(args)
	}
	if rf.State == "follower" && args.Term < rf.CurrentTerm {
		reply.Success = false
		rf.log("Denied AppendEntries, Term %d < %d\n", args.Term, rf.CurrentTerm)
	}
}

func (rf *Raft) appendToLog(args AppendEntriesArgs) bool {
	// translate 1 indexed PrevLogIndex to a 0 indexed local
	prevLogIndex := args.PrevLogIndex - 1
	rf.log("prevLogIndex = %d , log length = %d\n", prevLogIndex, len(rf.Log))

	// reply false if we don't have prevLogIndex
	if len(rf.Log) <= prevLogIndex {
		rf.log("Didn't have PrevLogIndex %d\n", args.PrevLogIndex)
		return false
	}

	rf.log("%+v\n", args)
	if len(rf.Log) > 0 && prevLogIndex >= 0 {
		// reply false if we do, but term doesn't match
		if rf.Log[prevLogIndex].Term != args.PrevLogTerm {
			rf.log("Terms don't match %d %d\n", rf.Log[prevLogIndex].Term, args.PrevLogTerm)
			return false
		}

		// check if entries after prevLogIndex
		// are different from ones in args.Entries
		// if so, delete them and all that follow
		rf.log("Checking for dirty entries on log %v\n", rf.Log)
		for ri, li := 0, prevLogIndex+1; ri < len(args.Entries) && li < len(rf.Log); ri, li = ri+1, li+1 {
			ourTerm := rf.Log[li].Term
			theirTerm := args.Entries[ri].Term
			if ourTerm != theirTerm {
				rf.Log = rf.Log[:li]
				break
			}
		}
	}

	// append the entries not already in the log
	// args.Entries := [3,4,5]
	// prev = 2
	// rf.Log = [1,2]
	// args.Entries[0:]

	// args.Entries := [3,4,5]
	// prev = 2
	// rf.Log = [1,2,3,4]
	// args.Entries[2:]

	// args.Entries := [2]
	// prev = 1
	// rf.Log = [1,2]
	// args.Entries[1:]

	// args.Entries = [1]
	// prev = 0
	// rf.Log = []
	// args.Entries[0:]
	firstNotInLog := len(rf.Log) - args.PrevLogIndex
	rf.log("rf.Log: %d, args.prev: %d, args.entries: %d\n", len(rf.Log), args.PrevLogIndex, len(args.Entries))
	if firstNotInLog < len(args.Entries) {
		entriesNotInLog := args.Entries[firstNotInLog:]
		rf.Log = append(rf.Log, entriesNotInLog...)
	}
	rf.log("After appending entries not in log: %v\n", rf.Log)

	// set commitIndex to min(leaderCommitIndex, index of last entry received)
	if args.LeaderCommit > rf.CommitIndex {
		lastEntryIndex := args.PrevLogIndex + len(args.Entries)
		newCommitIndex := min(args.LeaderCommit, lastEntryIndex)
		rf.log("New commit index = %d,\n", newCommitIndex)
		rf.commitAndApply(newCommitIndex)
		rf.log("CommitIndex: %d\n", rf.CommitIndex)
	}
	return true
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendRequest(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	term := -1
	isLeader := rf.State == "leader"

	if isLeader {
		index = len(rf.Log) + 1
		term = rf.CurrentTerm
		var entry LogEntry
		entry.Term = rf.CurrentTerm
		entry.Data = command
		rf.Log = append(rf.Log, entry)
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
	rf.log("Called kill on Node %d\n", rf.me)
	rf.stopHeartbeats()
	close(rf.heartbeatsTicker.stopChan)
	close(rf.heartbeatsTicker.restartChan)
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
	rf := &Raft{}
	rf.log("Node %d created\n", me)
	rf.t0 = time.Now()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.NextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i += 1 {
		rf.NextIndex[i] = 1
	}

	rf.MatchIndex = make([]int, len(peers))

	rf.State = "follower"
	timeout := randomTimeout()
	rf.startElectionTimeout(timeout)
	rf.startHeartbeatsTicker()
	rf.LastVoteTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) buildVoteRequest() RequestVoteArgs {
	return RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Log),
		LastLogTerm:  rf.lastEntry().Term,
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State == "leader" || rf.electionDisabled {
		return
	}
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.VotedFor = rf.me
	rf.LastVoteTerm = rf.CurrentTerm
	rf.VotesReceived = 1
	rf.State = "candidate"
	request := rf.buildVoteRequest()

	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go func(nodeId int, localTerm int) {
				var reply RequestVoteReply
				rf.log("%d -> RequestVoteRequest -> %d\n", rf.me, nodeId)
				rpcSuccess := rf.sendRequestVote(nodeId, request, &reply)

				rf.log("%d <- RequestVoteRequest <- %d: %t \n", rf.me, nodeId, reply.VoteGranted)
				rf.mu.Lock()
				if rpcSuccess && rf.State != "leader" { // this is a short-circuit
					if reply.VoteGranted {
						if localTerm == rf.CurrentTerm {
							rf.VotesReceived += 1
						} else {
							rf.log("Vote from %d ignored, Term was %d\n", nodeId, reply.Term)
						}
					}
					rf.log("Node %d got %d votes so far\n", rf.me, rf.VotesReceived)
					if rf.VotesReceived > len(rf.peers)/2 {
						rf.log("Node %d received a majority of the votes, becoming leader\n", rf.me)
						rf.State = "leader"

						// Reinitialize NextIndex and MatchIndex after election
						for i := 0; i < len(rf.peers); i += 1 {
							rf.NextIndex[i] = len(rf.Log) + 1
							rf.MatchIndex[i] = 0
						}

						rf.restartHeartbeatsTicker()
						rf.stopElectionTimeout()
					}
					if !reply.VoteGranted && reply.Term > rf.CurrentTerm {
						rf.State = "follower"
						rf.CurrentTerm = reply.Term
					}
				}
				rf.mu.Unlock()
			}(i, rf.CurrentTerm)
		}
	}

}

func (rf *Raft) startHeartbeatsTicker() {
	hbTicker := &HeartbeatsTicker{}
	hbTicker.ticker = time.NewTicker(100 * time.Millisecond)
	hbTicker.stopChan = make(chan struct{}, 100)
	hbTicker.restartChan = make(chan struct{}, 100)
	rf.heartbeatsTicker = hbTicker

	go func() {
		enabled := false
		for {
			select {
			case <-hbTicker.ticker.C:
				if enabled {
					rf.sendHeartbeats()
				}
			case <-hbTicker.stopChan:
				enabled = false
				hbTicker.ticker.Stop()
				rf.log("Stopped sending heartbeats!\n")
			case <-hbTicker.restartChan:
				if enabled {
					hbTicker.ticker.Stop()
				}
				enabled = true
				hbTicker.ticker = time.NewTicker(100 * time.Millisecond)
				rf.sendHeartbeats()
			}
		}
	}()
}

func (rf *Raft) stopHeartbeats() {
	rf.heartbeatsTicker.stopChan <- *new(struct{})
}

type HeartbeatUpdateMsg struct {
	NodeId   int
	Success  bool
	Reply    AppendEntriesReply
	LogIndex int
}

func (rf *Raft) sendHeartbeats() {
	rf.log("Node %d sending heartbeats, current log: %v\n", rf.me, rf.Log)
	rf.logCurrentState()

	for i := 0; i < len(rf.peers); i += 1 {
		if i == rf.me {
			continue
		}
		go func(nodeId int) {
			heartbeat := rf.buildAppendEntriesRequest(nodeId)
			var reply AppendEntriesReply
			rf.log("%d -> AppendEntries -> %d\n", rf.me, nodeId)
			rpcReceived := rf.sendAppendRequest(nodeId, heartbeat, &reply)
			if rpcReceived {
				rf.log("%d <- AppendEntries <- %d RECEIVED A RESPONSE\n", nodeId, rf.me)
			} else {
				rf.log("%d <- AppendEntries <- %d TIMEOUT\n", nodeId, rf.me)
			}

			logIndex := heartbeat.PrevLogIndex + len(heartbeat.Entries)

			if rpcReceived && rf.State == "leader" { // short-circuit (may have become a follower while being a leader and waiting for a heartbeat response)
				if !reply.Success && reply.Term == rf.CurrentTerm {
					// decrement nextIndex and retry
					rf.NextIndex[nodeId] -= 1
					rf.log("Heartbeat unsuccessful\n")
					rf.log("Reducing NextIndex of %d to %d\n", nodeId, rf.NextIndex[nodeId])
				} else if !reply.Success && reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.stopHeartbeats()
					rf.startElectionTimeout(randomTimeout())
					rf.State = "follower"
				} else if reply.Success {
					// update nextIndex
					rf.NextIndex[nodeId] = logIndex + 1
					rf.MatchIndex[nodeId] = logIndex
					rf.log("NextIndex = %v\n", rf.NextIndex)
					rf.log("MatchIndex = %v\n", rf.MatchIndex)

					rf.checkCommitted()
				}
			}

		}(i)

	}

}

func (rf *Raft) buildEntriesForPeer(nodeId int) []LogEntry {
	rf.log("Building entries for node %d\n", nodeId)
	nextIndex := rf.NextIndex[nodeId]
	if nextIndex-1 < len(rf.Log) {
		index := nextIndex - 1
		rf.log("index = %d\n", index)
		rf.log("log length = %d\n", len(rf.Log))
		elements := rf.Log[index:]
		return elements
	}
	return []LogEntry{}
}

func (rf *Raft) buildAppendEntriesRequest(nodeId int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	entries := rf.buildEntriesForPeer(nodeId)
	prevIndex := rf.NextIndex[nodeId] - 1
	prevTerm := 0
	if len(rf.Log) >= prevIndex && prevIndex > 0 {
		prevTerm = rf.Log[prevIndex-1].Term
	}
	return AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.CommitIndex,
	}
}

func (rf *Raft) checkCommitted() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := len(rf.Log)
	majority := len(rf.peers) / 2
	rf.log("Checking commited from [%d-%d)\n", lastIndex, rf.CommitIndex)
	for n := lastIndex; n > rf.CommitIndex; n -= 1 {
		counter := 1
		for _, idx := range rf.MatchIndex {
			rf.log("idx = %d\n", idx)
			if idx >= n {
				counter += 1
			}
		}
		rf.log("counter = %d , n = %d\n", counter, n)
		if counter > majority {
			rf.commitAndApply(n)
			rf.log(">> Committed = %d\n", n)
			break
		}
	}
}

func (rf *Raft) log(format string, args ...interface{}) {
	t := time.Since(rf.t0)
	fmtArgs := []interface{}{t / time.Millisecond, tabs(rf.me)}
	if len(args) > 0 {
		fmtArgs = append(fmtArgs, args...)
	}
	fmt.Printf("(%dms)%s"+format, fmtArgs...)
}

func (rf *Raft) commitAndApply(index int) {
	if rf.CommitIndex < index {
		go func() {
			// This goes inside a go routine because I think blocking on the
			// apply channel doesn't allow for other things to happen and the
			// tests depend on that
			rf.mu.Lock()
			rf.log("Last commit index = %d\n", rf.CommitIndex)
			for i := rf.CommitIndex + 1; i <= index; i += 1 {
				rf.CommitIndex = i
				rf.log("Applying %d (%d))\n", i, rf.Log[i-1])
				rf.applyCh <- ApplyMsg{
					Index:   i,
					Command: rf.Log[i-1].Data,
				}
				rf.LastApplied = i
			}
			rf.log("COMMITED UP TO %d \n", rf.Log[index-1].Data)
			rf.mu.Unlock()
		}()
	} else {
		rf.log("Didn't commit entry at %d because commit index is greater or equal (%d)\n", index, rf.CommitIndex)
	}
}
