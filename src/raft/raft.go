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
	applyCh          chan ApplyMsg

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
	ticker   *time.Ticker
	stopChan chan struct{}
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
				//fmt.Printf("%s*** STOPPING ELECTION TIMEOUT ***{\n", tabs(rf.me))
				ticker.Stop()
				//fmt.Printf("%s}*** STOPPING ELECTION TIMEOUT ***\n", tabs(rf.me))
			case <-rf.electionTimeout.restartChan:
				//fmt.Printf("%s*** RESTARTING ELECTION TIMEOUT ***{\n", tabs(rf.me))
				ticker.Stop()
				ticker = time.NewTicker(timeout)
				//fmt.Printf("%s}*** RESTARTING ELECTION TIMEOUT ***\n", tabs(rf.me))
			case <-ticker.C:
				fmt.Printf("%s*** ELECTION TIMEOUT ***{\n", tabs(rf.me))
				fmt.Printf("%sElection timeout! Node %d starting new election\n", tabs(rf.me), rf.me)
				rf.startElection()
				fmt.Printf("%s}*** ELECTION TIMEOUT ***\n", tabs(rf.me))
			}
		}
	}()

}

func (rf *Raft) stopElectionTimeout() {
	if rf.electionTimeout != nil {
		fmt.Printf("%sStopping election timeout\n", tabs(rf.me))
		rf.electionTimeout.stopChan <- *new(struct{})
		fmt.Printf("%sStopped election timeout\n", tabs(rf.me))
	} else {
		fmt.Printf("%sTried to stop electionTimeout, but none running\n", tabs(rf.me))
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

func (rf *Raft) VoteYes(args RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("%s%d -> RequestVoteReply -> %d: Vote\n", tabs(rf.me), rf.me, args.CandidateId)
	rf.VotedFor = args.CandidateId
	rf.AlreadyVoted = true
	reply.VoteGranted = true
	rf.startElectionTimeout(randomTimeout())
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	candidateIsAsUp2Date := (args.LastLogTerm != rf.lastEntry().Term && args.Term >= rf.CurrentTerm) ||
		(args.LastLogTerm == rf.lastEntry().Term && args.LastLogIndex >= len(rf.Log)-1) // page 8, paragraph before 5.4.2
	fmt.Printf("%s%d <- RequestVoteRequest <- %d: IsUpToDate? %t\n", tabs(rf.me), rf.me, args.CandidateId, candidateIsAsUp2Date)

	reply.Term = rf.CurrentTerm

	if rf.State == "candidate" && rf.CurrentTerm < args.Term {
		rf.State = "follower"
		rf.VoteYes(args, reply)
		return
	}

	if rf.CurrentTerm > args.Term || rf.AlreadyVoted {
		fmt.Printf("%s%d -> RequestVoteReply -> %d: AlreadyVoted or Term outdated\n", tabs(rf.me), rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if candidateIsAsUp2Date {
		rf.VoteYes(args, reply)
	} else {
		reply.VoteGranted = false
		fmt.Printf("%s%d -> RequestVoteReply -> %d: Candidate is not up2date\n", tabs(rf.me), rf.me, args.CandidateId)
	}
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm

	if (rf.State == "candidate" && args.Term >= rf.CurrentTerm) || args.Term > rf.CurrentTerm {
		if rf.State == "leader" {
			fmt.Printf("%sStepping down!\n", tabs(rf.me))
			rf.stopHeartbeats()
			fmt.Printf("%sTheoretically stopped sending heartbeats\n", tabs(rf.me))
		}
		rf.stopElectionTimeout()
		fmt.Printf("%s%d <- AppendEntries <- %d: New Leader Detected\n", tabs(rf.me), rf.me, args.LeaderId)
		rf.CurrentTerm = args.Term
		rf.State = "follower"
		rf.AlreadyVoted = false
	}
	if rf.State == "follower" && args.Term >= rf.CurrentTerm {
		rf.startElectionTimeout(randomTimeout())
		reply.Success = rf.appendToLog(args)
	}
	if rf.State == "follower" && args.Term < rf.CurrentTerm {
		reply.Success = false
	}
}

func (rf *Raft) appendToLog(args AppendEntriesArgs) bool {
	// reply false if we don't have prevLogIndex
	if len(rf.Log) < args.PrevLogIndex - 1 {
		fmt.Printf("%sDidn't have PrevLogIndex %d\n", tabs(rf.me), args.PrevLogIndex)
		return false
	}

	fmt.Printf("%s%+v\n", tabs(rf.me),args)
	if len(rf.Log) > 0 && args.PrevLogIndex > 0 {
		// reply false if we do, but term doesn't match
		if rf.Log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
			fmt.Printf("%sTerms dont match %d %d\n", tabs(rf.me), rf.Log[args.PrevLogIndex - 1].Term, args.PrevLogTerm)
			return false
		}

		// check if entries after prevLogIndex
		// are different from ones in args.Entries
		// if so, delete them and all that follow
		for i := args.PrevLogIndex; i < args.PrevLogIndex + len(args.Entries) && i < len(rf.Log); i += 1 {
			ourTerm := rf.Log[i].Term
			theirTerm := args.Entries[i - args.PrevLogIndex - 1].Term
			if ourTerm != theirTerm {
				rf.Log = rf.Log[:i]
				break
			}
		}
	}

	// append the entries not already in the log
	firstNotInLog := len(rf.Log) - args.PrevLogIndex
	entriesNotInLog := args.Entries[firstNotInLog:]
	rf.Log = append(rf.Log, entriesNotInLog...)

	fmt.Printf("%s%v\n", tabs(rf.me), rf.Log)

	// set commitIndex to min(leaderCommitIndex, index of last entry received)
	lastEntryIndex := args.PrevLogIndex + len(args.Entries)
	newCommitIndex := min(args.LeaderCommit, lastEntryIndex)
	rf.commitAndApply(newCommitIndex)
	fmt.Printf("%sCommitIndex: %d\n", tabs(rf.me), rf.CommitIndex)
	return true
}

func min(x, y int) int {
    if x < y {
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
		index = len(rf.Log)+1
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
	fmt.Printf("%sCalled kill on Node %d\n", tabs(rf.me), rf.me)
	rf.stopHeartbeats()
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
	rf.applyCh = applyCh
	rf.NextIndex = make([]int, len(peers))
	for i := 0; i <len(peers); i += 1 {
		rf.NextIndex[i] = 1
	}

	rf.MatchIndex = make([]int, len(peers))

	rf.State = "follower"
	timeout := randomTimeout()
	rf.startElectionTimeout(timeout)
	rf.startHeartbeatsTicker()

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
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.VotedFor = rf.me
	rf.AlreadyVoted = true
	rf.VotesReceived = 1
	rf.State = "candidate"
	request := rf.buildVoteRequest()

	peerCount := len(rf.peers)
	// @TODO define type for response
	repliesChan := make(chan struct {
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
				repliesChan <- struct {
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
		close(repliesChan)
	}()

	for pair := range repliesChan {
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
		rf.restartHeartbeatsTicker()
		rf.stopElectionTimeout()
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
				fmt.Printf("%sStopped sending heartbeats!\n", tabs(rf.me))
			case <- hbTicker.restartChan:
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
	fmt.Printf("%sNode %d sending heartbeats, log: %v\n", tabs(rf.me), rf.me, rf.Log)
	peerCount := len(rf.peers)
	hbsRepliesChan := make(chan *HeartbeatUpdateMsg, peerCount-1)
	var waitGroup sync.WaitGroup
	waitGroup.Add(peerCount - 1)

	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go func(nodeId int) {
				heartbeat := rf.buildAppendEntriesRequest(nodeId)
				var reply AppendEntriesReply
				fmt.Printf("%s%d -> AppendEntries -> %d\n", tabs(rf.me), rf.me, nodeId)
				rpcReceived := rf.sendAppendRequest(nodeId, heartbeat, &reply)
				if rpcReceived {
					fmt.Printf("%s%d <- AppendEntries <- %d OK\n", tabs(rf.me), rf.me, nodeId)
				} else {
					fmt.Printf("%s%d <- AppendEntries <- %d TIMEOUT\n", tabs(rf.me), rf.me, nodeId)
				}
				hbsRepliesChan <- &HeartbeatUpdateMsg {
					NodeId: nodeId,
					Success: rpcReceived,
					Reply: reply,
					LogIndex: heartbeat.PrevLogIndex + len(heartbeat.Entries),
				}
				waitGroup.Done()
			}(i)
		}
	}

	go func() {
		waitGroup.Wait()
		close(hbsRepliesChan)
	}()

	for msg := range hbsRepliesChan {
		fmt.Printf("%+v\n", msg)
		if msg.Success {
			if !msg.Reply.Success && msg.Reply.Term == rf.CurrentTerm {
				// decrement nextIndex and retry
				rf.NextIndex[msg.NodeId] -= 1
				fmt.Printf("Reducing NextIndex of %d to %d\n", msg.NodeId, rf.NextIndex[msg.NodeId])
			} else if msg.Reply.Success {
				// update nextIndex
				rf.NextIndex[msg.NodeId] = msg.LogIndex + 1
				rf.MatchIndex[msg.NodeId] = msg.LogIndex
				fmt.Printf("NextIndex[%d] is %d\n", msg.NodeId, rf.NextIndex[msg.NodeId])
				rf.checkCommitted()
			}
		}
	}
}

func (rf *Raft) buildEntriesForPeer(nodeId int) []LogEntry {
	nextIndex := rf.NextIndex[nodeId]
	if (nextIndex - 1 < len(rf.Log)){
		index := nextIndex - 1
		elements := rf.Log[index:]
		return elements
	}
	return []LogEntry{}
}


func (rf *Raft) buildAppendEntriesRequest(nodeId int) AppendEntriesArgs {
	entries := rf.buildEntriesForPeer(nodeId)
	prevIndex := rf.NextIndex[nodeId] - 1
	prevTerm := 0
	if len(rf.Log) >= prevIndex && prevIndex > 0 {
		prevTerm = rf.Log[prevIndex - 1].Term
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
	lastIndex := len(rf.Log)
	majority := len(rf.peers)/2
	for n := lastIndex; n > rf.CommitIndex; n -= 1 {
		counter := 1
		for idx := range rf.MatchIndex {
			if idx >= n {
				counter += 1
			}
		}
		if counter > majority {
			rf.commitAndApply(n);
			fmt.Printf(">> Committed = %d\n", n)
			break;
		}
	}
}

func (rf *Raft) commitAndApply(index int) {
	if rf.CommitIndex < index {
		rf.CommitIndex = index
		rf.applyCh <- *&ApplyMsg{
			Index: index,
			Command: rf.Log[index - 1].Data,
		}
		rf.LastApplied = index
	}
}