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
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm  int
	VotedFor     int
	AlreadyVoted bool
	Log          []LogEntry

	// Volatile
	CommitIndex      int
	LastApplied      int
	VotesReceived    int
	electionTimeout  *ElectionTimeout
	heartbeatsTicker *HeartbeatsTicker

	// Volatile data when leader
	NextIndex  []int
	MatchIndex []int

	State string
}

type LogEntry struct {
	Data int
	Term int
}

type ElectionTimeout struct {
	ticker   *time.Ticker
	stopChan chan struct{}
}

type HeartbeatsTicker struct {
	ticker   *time.Ticker
	stopChan chan struct{}
}

func (rf *Raft) startElectionTimeout(timeout time.Duration) {
	if rf.electionTimeout != nil {
		rf.stopElectionTimeout()
	}

	et := &ElectionTimeout{}
	et.ticker = time.NewTicker(timeout)
	et.stopChan = make(chan struct{})

	go func() {
		for {
			select {
			case <-et.ticker.C:
				fmt.Printf("%sElection timeout! Node %d starting new election\n", tabs(rf.me), rf.me)
				rf.startElection()
				return
			case <-et.stopChan:
				et.ticker.Stop()
				return
			}
		}

	}()

	rf.electionTimeout = et
}

func (rf *Raft) stopElectionTimeout() {
	rf.electionTimeout.stopChan <- *new(struct{})
	close(rf.electionTimeout.stopChan)
	rf.electionTimeout = nil
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here.
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
	// Your code here.
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.me)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
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
	// Your code here.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.me)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	candidateIsAsUp2Date := (args.LastLogTerm != rf.lastEntry().Term && args.Term >= rf.CurrentTerm) ||
		(args.LastLogTerm == rf.lastEntry().Term && args.LastLogIndex >= len(rf.Log)-1) // page 8, paragraph before 5.4.2
	fmt.Printf("%s%d <- RequestVoteRequest <- %d: IsUpToDate? %t\n", tabs(rf.me), rf.me, args.CandidateId, candidateIsAsUp2Date)

	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term || rf.AlreadyVoted {
		fmt.Printf("%s%d -> RequestVoteReply -> %d: AlreadyVoted or Term outdated\n", tabs(rf.me), rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if candidateIsAsUp2Date {
		fmt.Printf("%s%d -> RequestVoteReply -> %d: Vote\n", tabs(rf.me), rf.me, args.CandidateId)
		rf.VotedFor = args.CandidateId
		rf.AlreadyVoted = true
		reply.VoteGranted = true
		rf.startElectionTimeout(randomTimeout())
	} else {
		reply.VoteGranted = false
		fmt.Printf("%s%d -> RequestVoteReply -> %d: Candidate is not up2date\n", tabs(rf.me), rf.me, args.CandidateId)
	}
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.State == "leader" {
		fmt.Printf("%s>>Weird<<", tabs(rf.me))
	}
	if (rf.State == "candidate" && args.Term >= rf.CurrentTerm) || args.Term > rf.CurrentTerm {
		rf.stopElectionTimeout()
		fmt.Printf("%s%d <- AppendEntries <- %d: New Leader Detected\n", tabs(rf.me), rf.me, args.LeaderId)
		rf.CurrentTerm = args.Term
		rf.State = "follower"
		rf.AlreadyVoted = false
	}
	if rf.State == "follower" && args.Term >= rf.CurrentTerm {
		rf.startElectionTimeout(randomTimeout())
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
	index := -1
	term := -1
	isLeader := true

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
	fmt.Printf("%sNode %d created\n", tabs(me), me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.State = "follower"
	timeout := randomTimeout()
	rf.startElectionTimeout(timeout)

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

func (rf *Raft) buildAppendEntriesRequest(entries []LogEntry) AppendEntriesArgs {
	return AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.Log) + 1, // @TODO <- is this right?
		Entries:      entries,
		LeaderCommit: rf.CommitIndex,
	}
}

func (rf *Raft) startElection() {
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.VotedFor = rf.me
	rf.AlreadyVoted = true
	rf.VotesReceived = 1
	rf.State = "candidate"
	request := rf.buildVoteRequest()

	peerCount := len(rf.peers)
	sem := make(chan struct {
		int
		bool
		RequestVoteReply
	}, peerCount-1)
	var waitGroup sync.WaitGroup
	waitGroup.Add(peerCount - 1)

	for i := 0; i < peerCount; i += 1 {
		if i != rf.me {
			go func(nodeId int) {
				var reply RequestVoteReply
				fmt.Printf("%s%d -> RequestVoteRequest -> %d\n", tabs(rf.me), rf.me, nodeId)
				rpcSuccess := rf.sendRequestVote(nodeId, request, &reply)
				sem <- struct {
					int
					bool
					RequestVoteReply
				}{nodeId, rpcSuccess, reply}
				waitGroup.Done()
			}(i)
		}
	}

	go func() {
		waitGroup.Wait()
		close(sem)
	}()

	for pair := range sem {
		reply := pair.RequestVoteReply
		i := pair.int
		fmt.Printf("%s%d <- RequestVoteRequest <- %d: %t \n", tabs(rf.me), rf.me, i, reply.VoteGranted)
		if reply.VoteGranted {
			rf.VotesReceived += 1
		}
		if rf.VotesReceived > len(rf.peers)/2 {
			break
		}
		// @TODO check term in response?
	}

	fmt.Printf("%sNode %d got %d votes\n", tabs(rf.me), rf.me, rf.VotesReceived)
	if rf.VotesReceived > len(rf.peers)/2 {
		fmt.Printf("%sNode %d received a majority of the votes, becoming leader\n", tabs(rf.me), rf.me)
		rf.State = "leader"
		rf.sendHeartbeats()
		rf.scheduleHeartbeats()
	}
}

func (rf *Raft) scheduleHeartbeats() {
	rf.heartbeatsTicker = &HeartbeatsTicker{}
	rf.heartbeatsTicker.ticker = time.NewTicker(100 * time.Millisecond)
	rf.heartbeatsTicker.stopChan = make(chan struct{})

	go func() {
		for {
			select {
			case <-rf.heartbeatsTicker.ticker.C:
				rf.sendHeartbeats()
			case <-rf.heartbeatsTicker.stopChan:
				rf.heartbeatsTicker.ticker.Stop()
				return
			}
		}
	}()
}

func (rf *Raft) stopHeartbeats() {
	rf.heartbeatsTicker.stopChan <- *new(struct{})
	close(rf.heartbeatsTicker.stopChan)
}

func (rf *Raft) sendHeartbeats() {
	heartbeat := rf.buildAppendEntriesRequest([]LogEntry{})
	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			var reply AppendEntriesReply
			rf.sendAppendRequest(i, heartbeat, &reply) // @TODO handle return error?
			// @TODO what to do with the answer in this case
		}
	}
}
