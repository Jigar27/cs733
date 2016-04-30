package main

import (
		//"fmt"
		"time"
		"errors"
		"math/rand"
)




//const (
	
//	heartbeatTimeout = 100 * time.Millisecond 
//	electionTimeout = 2 * heartbeatTimeout
	
//)

type Event interface{}

type Action interface{}

type Timeout struct {

}



type SetTimer struct {
	Timer int64
}

type AppendData struct	{
	CommandName []byte
	//uniqueId int // this id is given by the clients 
}



type LogItems struct {
	Term int
	CommandName []byte
	//uniqueId uint64  // 
	//commited bool
}



type RequestAppendEntries struct {
	SenderID int	
	SenderTerm int
	PrevLogIndex int64
	PrevLogTerm int
	
	//leaderId uint64
	LogEntries []LogItems
	CommitIndex int64
}

type ResponseAppendEntries struct {
	SenderID int	
	SenderTerm int 	
	SenderLastMatchedIndex int64
	Success bool
	
}

type RequestVote struct {
	Term int
	CandidateId int
	LastLogIndex int64
	LastLogTerm int
	
}

type ResponseVote struct {
	SenderID int
	SenderTerm int
	Success bool 
}


type SendAction struct {
	IntendedReceiverID int
	Event Event
}


type LogStoreAction struct {
	Index int64
	Entry LogItems
}

type StateStoreAction struct {
	Term int
	VotedFor int
	LastMatchedIndex int64
}

type CommitAction struct {
	LeaderID int
	CommitIndex int64
	CommittedEntry LogItems
	ErrorMsg error 
}

//type SendLeaderInfoAction struct{
//	leaderID int
//	msg string
//}
type RaftStateMachineServer struct {
	// Persistent: Applicable to servers on any state
	term int
	votedFor int
	log []LogItems
	id int
	peerIds []int
	electionTimeoutPeriod int64
	heartbeatTimeoutPeriod int64

	// Persistent: Applicable to servers on Follower state
	lastMatchedIndex int64 /* In a term, lastRepIndex denotes the last index upto which the log matches the leader's log (in the current term)*/

	// Non-Persistent: Applicable to servers on any state
	state string
	CommitIndex int64
	leaderId int
	lastLogIndex int64
	lastLogTerm int
	clusterSize int

	// Non-Persistent: Applicable to servers on Candidate state
	voteReceived map[int]int /* For a server, 0 : No voteResponse received, 1 : voteGranted, -1 : voteDenied */

	// Non-Persistent: Applicable to a server on Leader state 
	// Maps from senderId to corresponding variable
	nextIndex map[int]int64
	matchIndex map[int]int64
}


//type 


/*func newLog() *LogItems {
	l := &LogItems{[]LogItems}
	return l
}*/


func Random(min, max int64) int64 {
    rand.Seed(time.Now().Unix())
    return int64(rand.Intn(int(max) - int(min) + int(min)))
}

/*func(server  *Server) IntializeNewServer(id int,leader int,votedFor int, term int, peerIDs []int,
 										state string, active bool,electionTimeout int64, heartbeatTimeout int64,log []LogItems,commitIndex int64,
 										lastLogIndex int64,lastLogTerm int,lastMatchedIndex int64,voteReceived map[int]int,nextIndex map[int]int64,matchIndex map[int]int64)  {

	// create log for the server
	//log := newLog()
	//server Server
	server = &Server { id,leader,votedFor,term,	peerIDs, state, active, electionTimeout, heartbeatTimeout, log, commitIndex, lastLogIndex, 	lastLogTerm,
						len(peerIDs)+1, lastMatchedIndex, voteReceived,
						nextIndex, matchIndex }
}*/

func (rsmServer *RaftStateMachineServer) init(term int, votedFor int, log []LogItems, id int, peerIds []int, electionAlarmPeriod int64, heartbeatAlarmPeriod int64, lastMatchedIndex int64, state string, commitIndex int64, leaderId int, lastLogIndex int64, lastLogTerm int, voteReceived map[int]int, nextIndex map[int]int64, matchIndex map[int]int64) {
	rsmServer.term = term
	rsmServer.votedFor = votedFor
	rsmServer.log = log
	rsmServer.id = id
	rsmServer.peerIds = peerIds	
	rsmServer.electionTimeoutPeriod = electionAlarmPeriod
	rsmServer.heartbeatTimeoutPeriod = heartbeatAlarmPeriod
	rsmServer.lastMatchedIndex = lastMatchedIndex
	rsmServer.state = state
	rsmServer.CommitIndex = commitIndex
	rsmServer.leaderId = leaderId
	rsmServer.lastLogIndex = lastLogIndex
	rsmServer.lastLogTerm = lastLogTerm
	rsmServer.clusterSize = len(peerIds) + 1
	rsmServer.voteReceived = voteReceived
	rsmServer.nextIndex = nextIndex
	rsmServer.matchIndex = matchIndex
}

func(rsmServer *RaftStateMachineServer) RequestAppendEntriesRPC(serverIDs []int) []Action {
	var actions []Action
	var prevLogIndex int64
	var prevLogTerm int
	for i:=0; i<len(serverIDs);i++ {
		prevLogIndex = rsmServer.nextIndex[serverIDs[i]]-1
		if prevLogIndex < 0 {
			prevLogTerm = 0
		} else {
			prevLogTerm = rsmServer.log[prevLogIndex].Term	
		}
		
		logEntries := rsmServer.log[rsmServer.nextIndex[serverIDs[i]]:]
		requestAppend := RequestAppendEntries {rsmServer.id,rsmServer.term,prevLogIndex,prevLogTerm,logEntries,rsmServer.CommitIndex}

		send := SendAction{serverIDs[i],requestAppend}
		actions = append(actions, send)   
	}

	return actions
}



func (rsmServer *RaftStateMachineServer) RequestVoteRPC(serverIDs []int) []Action {
	var actions []Action
	for i:=0; i< len(serverIDs); i++ {
		reqVote := RequestVote{rsmServer.term,rsmServer.id,rsmServer.lastLogIndex,rsmServer.lastLogTerm}
		send := SendAction{serverIDs[i],reqVote}
		actions = append(actions, send)
	}

	return actions
}



func (rsmServer *RaftStateMachineServer) HandleEvents(event Event) []Action{
	var actions []Action
	switch event.(type) {
	case RequestAppendEntries:
		actions = rsmServer.HandleRequestAppendEntries(event)

	case ResponseAppendEntries:
		actions = rsmServer.HandleResponseAppendEntries(event)

	case RequestVote:
		actions = rsmServer.HandleRequestVote(event)

	case ResponseVote:
		actions = rsmServer.HandleResponseVote(event)

	case AppendData:
		actions = rsmServer.HandleAppendData(event)

	case Timeout:
		actions = rsmServer.HandleTimeout(event)
	}

	return actions
}


func(rsmServer *RaftStateMachineServer) HandleRequestAppendEntries(event Event) []Action {
	var actions []Action
	var commit CommitAction
	/*var action Action*/
	requestAppendEntry,_ := event.(RequestAppendEntries)
	senderID := requestAppendEntry.SenderID
	senderTerm := requestAppendEntry.SenderTerm
	prevLogIndex := requestAppendEntry.PrevLogIndex
	prevLogTerm := requestAppendEntry.PrevLogTerm
	logEntries := requestAppendEntry.LogEntries
	commitIndex := requestAppendEntry.CommitIndex

	//act accordingly the state of the server

	switch rsmServer.state {
	
		
	case "follower" :
		if rsmServer.term > senderTerm {
			//since this server has a higher term then sender server, the sender server might be the old leader
			responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
			send := SendAction{senderID,responseAppendEntry}
			actions = append(actions,send) 
		} else {
			//since this request has come from a legitimate leader it also serves as a heartbeat from tha leader so i need to restart the timer to wait for the next heartbeat
			timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)
			// the server is not old. if this server's term is older than leader's term then update the state of this server
			if rsmServer.term < senderTerm {
				rsmServer.term = senderTerm
				rsmServer.leaderId = senderID
				rsmServer.lastMatchedIndex = -1
				rsmServer.votedFor = -1
				statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,statestore)	
			}
			


			//its time for append entries consistency check
			//if this server accepts this append entry then it means that this server's log is matched till this new index with leader's log
			//this server accepts this append entry only when this server's last log index and last log term matches with leader's prev log index and prev log term. 

			//if rsmServer.log[prevLogIndex].term != prevLogTerm {
			if (prevLogIndex > rsmServer.lastLogIndex) || (prevLogIndex > -1 && (rsmServer.log[prevLogIndex].Term != prevLogTerm)) {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
				send := SendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			

			} else if rsmServer.lastMatchedIndex >= prevLogIndex + int64(len(logEntries)){
				if rsmServer.CommitIndex < rsmServer.lastLogIndex && rsmServer.CommitIndex < commitIndex {
					if rsmServer.lastLogIndex < commitIndex {
						rsmServer.CommitIndex = rsmServer.lastLogIndex
					} else {
						rsmServer.CommitIndex = commitIndex
					}

					commit = CommitAction{rsmServer.leaderId, rsmServer.CommitIndex, rsmServer.log[rsmServer.CommitIndex], nil}
					actions = append(actions,commit)
				}

				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction {senderID,responseAppendEntry}
				actions = append(actions,send)

			
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				rsmServer.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0 { // this means that this append request rpc contains data. not just a heartbeat signal.
					rsmServer.lastLogIndex = prevLogIndex
					rsmServer.log = rsmServer.log[:rsmServer.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					rsmServer.lastLogIndex++
					rsmServer.log = append (rsmServer.log,logEntries[i])
					rsmServer.lastLogTerm = rsmServer.log[rsmServer.lastLogIndex].Term
					rsmServer.lastMatchedIndex = rsmServer.lastLogIndex
					
					logStore := LogStoreAction{rsmServer.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact 
				stateStore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,stateStore)
				if rsmServer.CommitIndex < commitIndex {
					if rsmServer.CommitIndex < rsmServer.lastLogIndex{
						if commitIndex <= rsmServer.lastLogIndex{
							rsmServer.CommitIndex = commitIndex
						} else{
							rsmServer.CommitIndex = rsmServer.lastLogIndex
						}
						cm := CommitAction{rsmServer.leaderId,rsmServer.CommitIndex,rsmServer.log[rsmServer.CommitIndex],nil}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction {senderID,responseAppendEntry}
				actions = append(actions,send)
			}
		}

	case "candidate" :
		if rsmServer.term > senderTerm {
			responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
			send := SendAction{senderID,responseAppendEntry}
			actions = append(actions,send) 
		} else {
			//since this request has come from a legitimate leader it also serves as a heartbeat from tha leader so i need to restart the timer to wait for the next heartbeat
			//plus this server is in candidate state so it has to revert back its state to follower
			rsmServer.state = "follower"
			timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)
			/*rsmServer.term = senderTerm
			rsmServer.leaderId = senderID
			rsmServer.lastMatchedIndex = -1
*/


			if rsmServer.term < senderTerm {
				rsmServer.term = senderTerm
				rsmServer.leaderId = senderID
				rsmServer.lastMatchedIndex = -1
				rsmServer.votedFor = -1
				statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,statestore)	
			}


			if (prevLogIndex > rsmServer.lastLogIndex) || (prevLogIndex > -1 && (rsmServer.log[prevLogIndex].Term != prevLogTerm)) {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
				send := SendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			

			} else if rsmServer.lastMatchedIndex >= prevLogIndex + int64(len(logEntries)){
				if rsmServer.CommitIndex < rsmServer.lastLogIndex && rsmServer.CommitIndex < commitIndex {
					if rsmServer.lastLogIndex < commitIndex {
						rsmServer.CommitIndex = rsmServer.lastLogIndex
					} else {
						rsmServer.CommitIndex = commitIndex
					}

					commit = CommitAction{rsmServer.leaderId, rsmServer.CommitIndex, rsmServer.log[rsmServer.CommitIndex], nil}
					actions = append(actions,commit)
				}

				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction {senderID,responseAppendEntry}
				actions = append(actions,send)

			
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				rsmServer.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0 { // this means that this append request rpc contains data. not just a heartbeat signal.
					rsmServer.lastLogIndex = prevLogIndex
					rsmServer.log = rsmServer.log[:rsmServer.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					rsmServer.lastLogIndex++
					rsmServer.log = append (rsmServer.log,logEntries[i])
					rsmServer.lastLogTerm = rsmServer.log[rsmServer.lastLogIndex].Term
					rsmServer.lastMatchedIndex = rsmServer.lastLogIndex
					
					logStore := LogStoreAction{rsmServer.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact 
				stateStore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,stateStore)
				if rsmServer.CommitIndex < commitIndex {
					if rsmServer.CommitIndex < rsmServer.lastLogIndex{
						if commitIndex <= rsmServer.lastLogIndex{
							rsmServer.CommitIndex = commitIndex
						} else{
							rsmServer.CommitIndex = rsmServer.lastLogIndex
						}
						cm := CommitAction{rsmServer.leaderId,rsmServer.CommitIndex,rsmServer.log[rsmServer.CommitIndex],nil}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction {senderID,responseAppendEntry}
				actions = append(actions,send)
			}

			//its time for append entries consistency check
			//if this server accepts this append entry then it means that this server's log is matched till this new index with leader's log
			//this server accepts this append entry only when this server's last log index and last log term matches with leader's prev log index and prev log term. 

			/*if rsmServer.log[prevLogIndex].Term != prevLogTerm {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
				send := SendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				rsmServer.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0  { // this means that this append request rpc contains data. not just a heartbeat signal.
					rsmServer.lastLogIndex = prevLogIndex
					rsmServer.log = rsmServer.log[:rsmServer.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					rsmServer.log = append (rsmServer.log,logEntries[i])
					rsmServer.lastLogIndex++
					rsmServer.lastLogTerm = rsmServer.log[rsmServer.lastLogIndex].Term
					rsmServer.lastMatchedIndex = rsmServer.lastLogIndex
					//store this action
					logStore := LogStoreAction{rsmServer.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact in statestore
				stateStore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,stateStore)
				if rsmServer.CommitIndex < commitIndex {
					if rsmServer.CommitIndex < rsmServer.lastLogIndex{
						if commitIndex <= rsmServer.lastLogIndex{
							rsmServer.CommitIndex = commitIndex
						} else{
							rsmServer.CommitIndex = rsmServer.lastLogIndex
						}
						cm := CommitAction{rsmServer.leaderId,rsmServer.CommitIndex,rsmServer.log[rsmServer.CommitIndex],nil}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction{senderID,responseAppendEntry}
				actions = append(actions,send)
			}*/

		}

	case "leader" :
		if rsmServer.term > senderTerm {
			responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
			send := SendAction{senderID,responseAppendEntry}
			actions = append(actions,send) 
		} else {
			//since this request has come from a legitimate leader it also serves as a heartbeat from tha leader so i need to restart the timer to wait for the next heartbeat
			//plus this server is in candidate state so it has to revert back its state to follower
			rsmServer.state = "follower"
			timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)



			if rsmServer.term < senderTerm {
				rsmServer.term = senderTerm
				rsmServer.leaderId = senderID
				rsmServer.lastMatchedIndex = -1
				rsmServer.votedFor = -1
				statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,statestore)	
			}


			if (prevLogIndex > rsmServer.lastLogIndex) || (prevLogIndex > -1 && (rsmServer.log[prevLogIndex].Term != prevLogTerm)) {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
				send := SendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			

			} else if rsmServer.lastMatchedIndex >= prevLogIndex + int64(len(logEntries)){
				if rsmServer.CommitIndex < rsmServer.lastLogIndex && rsmServer.CommitIndex < commitIndex {
					if rsmServer.lastLogIndex < commitIndex {
						rsmServer.CommitIndex = rsmServer.lastLogIndex
					} else {
						rsmServer.CommitIndex = commitIndex
					}

					commit = CommitAction{rsmServer.leaderId, rsmServer.CommitIndex, rsmServer.log[rsmServer.CommitIndex], nil}
					actions = append(actions,commit)
				}

				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction {senderID,responseAppendEntry}
				actions = append(actions,send)

			
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				rsmServer.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0 { // this means that this append request rpc contains data. not just a heartbeat signal.
					rsmServer.lastLogIndex = prevLogIndex
					rsmServer.log = rsmServer.log[:rsmServer.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					rsmServer.lastLogIndex++
					rsmServer.log = append (rsmServer.log,logEntries[i])
					rsmServer.lastLogTerm = rsmServer.log[rsmServer.lastLogIndex].Term
					rsmServer.lastMatchedIndex = rsmServer.lastLogIndex
					
					logStore := LogStoreAction{rsmServer.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact 
				stateStore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,stateStore)
				if rsmServer.CommitIndex < commitIndex {
					if rsmServer.CommitIndex < rsmServer.lastLogIndex{
						if commitIndex <= rsmServer.lastLogIndex{
							rsmServer.CommitIndex = commitIndex
						} else{
							rsmServer.CommitIndex = rsmServer.lastLogIndex
						}
						cm := CommitAction{rsmServer.leaderId,rsmServer.CommitIndex,rsmServer.log[rsmServer.CommitIndex],nil}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction {senderID,responseAppendEntry}
				actions = append(actions,send)
			}
			



			/*rsmServer.term = senderTerm
			rsmServer.leaderId = senderID
			rsmServer.lastMatchedIndex = -1
*/

			//its time for append entries consistency check
			//if this server accepts this append entry then it means that this server's log is matched till this new index with leader's log
			//this server accepts this append entry only when this server's last log index and last log term matches with leader's prev log index and prev log term. 

			/*if rsmServer.log[prevLogIndex].Term != prevLogTerm {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,false}
				send := SendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				rsmServer.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0 { // this means that this append request rpc contains data. not just a heartbeat signal.
					rsmServer.lastLogIndex = prevLogIndex
					rsmServer.log = rsmServer.log[:rsmServer.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					rsmServer.log = append (rsmServer.log,logEntries[i])
					rsmServer.lastLogIndex++
					rsmServer.lastLogTerm = rsmServer.log[rsmServer.lastLogIndex].Term
					rsmServer.lastMatchedIndex = rsmServer.lastLogIndex
					//store this action
					logStore := LogStoreAction{rsmServer.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact in statestore
				stateStore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,stateStore)
				if rsmServer.CommitIndex < commitIndex {
					if rsmServer.CommitIndex < rsmServer.lastLogIndex{
						if commitIndex <= rsmServer.lastLogIndex{
							rsmServer.CommitIndex = commitIndex
						} else{
							rsmServer.CommitIndex = rsmServer.lastLogIndex
						}
						cm := CommitAction{rsmServer.leaderId,rsmServer.CommitIndex,rsmServer.log[rsmServer.CommitIndex],nil}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := ResponseAppendEntries{rsmServer.id,rsmServer.term,rsmServer.lastMatchedIndex,true}
				send := SendAction {senderID,responseAppendEntry}
				actions = append(actions,send)
			}
*/
		}


	}

	return actions
}


func(rsmServer *RaftStateMachineServer) HandleResponseAppendEntries(event Event) []Action{
	var actions []Action
	responseAppendEntry,_ := event.(ResponseAppendEntries)
	senderID := responseAppendEntry.SenderID
	senderTerm := responseAppendEntry.SenderTerm
	senderLastMatchedIndex := responseAppendEntry.SenderLastMatchedIndex
	success := responseAppendEntry.Success

	switch rsmServer.state{
	case "follower":
			//follower can't process this RPC
	case "candidate":
			//candidate can't process this RPC
	case "leader":
		//if leader is old leader then ..
		if rsmServer.term < senderTerm {
			rsmServer.state = "follower"
			rsmServer.term = senderTerm
			rsmServer.votedFor = -1
			rsmServer.lastMatchedIndex = -1
			timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)
			stateStore := StateStoreAction{rsmServer.term, rsmServer.votedFor, rsmServer.lastMatchedIndex}
			actions = append(actions, stateStore)
		} else if success == true {
			rsmServer.matchIndex[senderID] = senderLastMatchedIndex
			rsmServer.nextIndex[senderID] = rsmServer.matchIndex[senderID] + 1
			noCopy := 1 // since leader has already an original copy of that.
			for i := rsmServer.matchIndex[senderID] ; i > rsmServer.CommitIndex ; i-- {
				for j:=0; j<len(rsmServer.peerIds); j++ {
					if rsmServer.matchIndex[j] >= i {
						noCopy ++
						if noCopy > (len(rsmServer.peerIds)+1)/2 {
							rsmServer.CommitIndex = i 
							cm := CommitAction{rsmServer.id,rsmServer.CommitIndex,rsmServer.log[rsmServer.CommitIndex],nil}
							actions = append(actions,cm)
							break
						}
					}
				}
				noCopy = 1
			}
		} else if success == false {	
			rsmServer.nextIndex[senderID]--
			actions= append(actions,rsmServer.RequestAppendEntriesRPC([]int{senderID}))


		} //else {
			//rsmServer.nextIndex[senderID]--
			// should call requestappendentriesRPC with this new nextindex
			//actions = append(actions,rsmServer.RequestAppendEntriesRPC([]int{senderID}))
		//}


	}

	return actions
}


func(rsmServer *RaftStateMachineServer) HandleRequestVote(event Event) []Action{
	var actions []Action
	requestVote := event.(RequestVote)
	candidateTerm := requestVote.Term
	candidateId := requestVote.CandidateId
	candidateLastLogIndex := requestVote.LastLogIndex
	candidateLastLogTerm := requestVote.LastLogTerm

	switch rsmServer.state{
	case "follower":
		// if you receive a vote request from a candidate whose term is higher than you then it is the time to change the term
		if rsmServer.term < candidateTerm {
			rsmServer.term = candidateTerm
			//since this is newer term this server has not voted to anyone in this newer term
			rsmServer.votedFor = -1
			//newer term may have new leader altogether so this server has not matched anything with newer leader yet
			rsmServer.lastMatchedIndex = -1
			statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
			actions = append(actions,statestore)
		}

		//check whether the candidate is as modern and knowledgeble as you 
		if rsmServer.term > candidateTerm { 
			resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
			send := SendAction{candidateId,resVote}
			actions = append(actions,send)
		} else if rsmServer.lastLogTerm > candidateLastLogTerm{
			resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
			send := SendAction{candidateId,resVote}
			actions = append(actions,send)
		} else if rsmServer.lastLogTerm == candidateLastLogTerm && rsmServer.lastLogIndex > candidateLastLogIndex {
			resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
			send := SendAction{candidateId,resVote}
			actions = append(actions,send)
		} else if rsmServer.votedFor != -1 && rsmServer.votedFor != rsmServer.id { // if server has voted to anyone or server itself is a candidate then deny vote
			resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
			send := SendAction{candidateId,resVote}
			actions = append(actions,send)
		} else { // vote this candidate
			timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)
			// server state is changed
			rsmServer.votedFor = candidateId
			statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
			actions = append(actions,statestore)


			resVote := ResponseVote{rsmServer.id,rsmServer.term,true}
			send := SendAction{candidateId,resVote}
			actions = append(actions,send)

			
		}

	case "candidate":
		if rsmServer.term < candidateTerm {
			rsmServer.state = "follower"
			rsmServer.term = candidateTerm
			rsmServer.votedFor = -1
			rsmServer.lastMatchedIndex = -1
			timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)
			statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
			actions := append(actions,statestore)

			////check whether the candidate is as modern and knowledgeble as you 
			if rsmServer.lastLogTerm > candidateLastLogTerm {
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if rsmServer.lastLogTerm == candidateLastLogTerm && rsmServer.lastLogIndex > candidateLastLogIndex {
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if rsmServer.votedFor != -1 && rsmServer.votedFor != rsmServer.id { // if server has voted to anyone or server itself is a candidate then deny vote
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
			} else{
				resVote := ResponseVote{rsmServer.id,rsmServer.term,true}
				send := SendAction{candidateId,resVote}
				actions := append(actions,send)

				// server state is changed
				rsmServer.votedFor = candidateId
				statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,statestore)
			}
		} else if rsmServer.term == candidateTerm{
			// since this server is in candidate state and its term is actully the current term of the system it can't revote 
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
		} else{
			// in this case , this server is modern than the candidate server so deny the vote
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
		}

	case "leader":
		if rsmServer.term < candidateTerm {
			rsmServer.state = "follower"
			rsmServer.term = candidateTerm
			rsmServer.votedFor = -1
			rsmServer.lastMatchedIndex = -1
			statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
			actions := append(actions,statestore)

			////check whether the candidate is as modern and knowledgeble as you 
			if rsmServer.lastLogTerm > candidateLastLogTerm {
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if rsmServer.lastLogTerm == candidateLastLogTerm && rsmServer.lastLogIndex > candidateLastLogIndex {
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if rsmServer.votedFor != -1 && rsmServer.votedFor != rsmServer.id { // if server has voted to anyone or server itself is a candidate then deny vote
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
			} else{
				resVote := ResponseVote{rsmServer.id,rsmServer.term,true}
				send := SendAction{candidateId,resVote}
				actions := append(actions,send)

				// server state is changed
				rsmServer.votedFor = candidateId
				statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
				actions = append(actions,statestore)
			}
		} else if rsmServer.term == candidateTerm{
			//candidate's term is the same as this server's term.
			//to ensure the safety property "every term consists of atmost one leader" this server as being the leader should deny the candidate
			resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
			send := SendAction{candidateId,resVote}
			actions = append(actions,send)
		} else{
			// in this case , this server is modern than the candidate server so deny the vote
				resVote := ResponseVote{rsmServer.id,rsmServer.term,false}
				send := SendAction{candidateId,resVote}
				actions = append(actions,send)
		}
	}

	return actions
}

func(rsmServer *RaftStateMachineServer) HandleResponseVote(event Event) []Action{
	var actions []Action
	var noVoteGranted,noVoteDenied int
	resVote := event.(ResponseVote)
	senderID := resVote.SenderID
	senderTerm := resVote.SenderTerm
	senderResponse := resVote.Success

	switch rsmServer.state {
	case "follower":
		if rsmServer.term < senderTerm {
			rsmServer.term = senderTerm
			rsmServer.votedFor = -1
			rsmServer.lastMatchedIndex = -1

			statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
			actions = append (actions,statestore)
		}

	case "candidate":
		//we have to maintain 2 variables in order to decide whether this candidate server is liable to become a server or not 
		noVoteGranted = 1 // since server votes for itself
		noVoteDenied = 0

		for i:=0; i<len(rsmServer.peerIds);i++ {
			if rsmServer.voteReceived[i] == 1 {
				noVoteGranted++
				continue
			}
			if rsmServer.voteReceived[i] == -1 {
				noVoteDenied++
				continue
			}
		}

		if rsmServer.term < senderTerm {
			rsmServer.state = "follower"
			rsmServer.term = senderTerm
			rsmServer.votedFor = -1
			rsmServer.lastMatchedIndex = -1
			timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)
			statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
			actions = append (actions,statestore)
		} else if rsmServer.term == senderTerm { 

			if senderResponse == true {
			// now check whether this server has got enough vote to become leader
			rsmServer.voteReceived[senderID] = 1
			noVoteGranted++

			if noVoteGranted > (len(rsmServer.peerIds)+1)/2 {
				//now this server can become leader of the system
				rsmServer.state = "leader"
				rsmServer.leaderId = rsmServer.id
				// now this server has to maintain nextindex and last match index for each follower
				rsmServer.nextIndex = make(map[int]int64)
				rsmServer.matchIndex = make(map[int]int64)
				//initialize next index value for each follower equal to this server lastLogIndex+1
				//initialize match index value for each follower equal to -1
				for i:=0;i<len(rsmServer.peerIds);i++ {
					rsmServer.nextIndex[i]=rsmServer.lastLogIndex+1
					rsmServer.matchIndex[i]= -1
				}
				// next thing this server has to do is replicate its log onto each follower
				timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
				actions = append(actions,timer)
				actions = append(actions,rsmServer.RequestAppendEntriesRPC(rsmServer.peerIds))
			}
		} else if senderResponse == false {
			//reflect this fact 
			rsmServer.voteReceived[senderID] = -1
			noVoteDenied++

			if noVoteDenied > (len(rsmServer.peerIds)+1)/2 {
				rsmServer.state = "follower"
				timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
				actions = append(actions,timer)

			}
		}
		}
		//noVoteReceived := noVoteDenied + noVoteGranted
		//what happened when timeout occu

	case "leader":
		if rsmServer.term < senderTerm {
			rsmServer.state = "follower"
			rsmServer.term = senderTerm
			rsmServer.votedFor = -1
			rsmServer.lastMatchedIndex = -1
			statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
			actions = append (actions,statestore)
		}
	}

	return actions
}


func(rsmServer *RaftStateMachineServer) HandleAppendData(event Event) []Action {
	var actions []Action
	var commandName []byte
	var appendDataST AppendData
	//var entry LogItems
	appendDataST = event.(AppendData)
	commandName = appendDataST.CommandName

	switch rsmServer.state {
	case "follower" :
		//If this server is in follower state then it has nothing to do with this event.
		//If this server knows who is the leader of this system then simply responds with leader information
		//sendLI := SendLeaderInfoAction{rsmServer.leaderId,"CONTACT_LEADER"}

		commit := CommitAction{rsmServer.leaderId,-1,LogItems{},errors.New("ERR_CONTACT_LEADER")}
		actions = append(actions,commit)

	case "candidate" :
		//Since this server is in candidate state, from this server's point of view there is no leader exist in the system
		commit := CommitAction{-1,-1,LogItems{},errors.New("ERR_TRY_LATER")}
		actions = append(actions,commit)
	
	case "leader" :
		//leader will make a new entry into its log and store commandName
		rsmServer.lastLogIndex++
		//rsmServer.log[rsmServer.lastLogIndex].term = rsmServer.term
		//rsmServer.log[rsmServer.lastLogIndex].commandName = commandName
		//rsmServer.lastLogTerm = rsmServer.log[rsmServer.lastLogIndex].term
		rsmServer.lastLogTerm=rsmServer.term
		
		rsmServer.log = append(rsmServer.log, LogItems{rsmServer.term, commandName})
		//rsmServer.lastLogTerm = rsmServer.log[rsmServer.lastLogIndex].term
		logStore := LogStoreAction{rsmServer.lastLogIndex, rsmServer.log[rsmServer.lastLogIndex]}
		actions = append(actions,logStore)

		//after appending its own log, next action that this leader should take is to replicate this entry into its follower's log
		actions = append(actions,rsmServer.RequestAppendEntriesRPC(rsmServer.peerIds))
	}

	return actions
}


func(rsmServer *RaftStateMachineServer) HandleTimeout(event Event) []Action{
	var actions []Action

	//this event tells that previous timeout interval elapsed so restart new interval for timeout
	
	switch rsmServer.state{
	case "follower":
		timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)

		//since timeout interval has elapsed, first guess of this server is that no leader exists in the system
		rsmServer.state = "candidate"
		rsmServer.term++
		rsmServer.votedFor = rsmServer.id
		rsmServer.lastMatchedIndex = -1
		rsmServer.voteReceived = make(map[int]int)
		// without following step you would get error while testing "nil map assignment"
		//for i:= 0; i < len(rsmServer.peerIds); i++ {
		//	rsmServer.voteReceived[rsmServer.peerIds[i]] = 0
		//}

		statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
		actions = append(actions,statestore)
		//ask other servers to vote for itself
		actions = append(actions,rsmServer.RequestVoteRPC(rsmServer.peerIds))

	case "candidate":
		timer := SetTimer{int64(Random(int64(rsmServer.heartbeatTimeoutPeriod),int64(rsmServer.electionTimeoutPeriod)))}
			actions = append(actions,timer)

		//retry again for the next timeout interval
		rsmServer.term++
		rsmServer.votedFor = rsmServer.id
		rsmServer.lastMatchedIndex = -1
		//refresh voteReceived
		rsmServer.voteReceived = make(map[int]int)
		for i:=0; i<len(rsmServer.peerIds); i++ {
			rsmServer.voteReceived[i] = 0
		}

		statestore := StateStoreAction{rsmServer.term,rsmServer.votedFor,rsmServer.lastMatchedIndex}
		actions = append(actions,statestore)
		//ask other servers to vote for itself
		actions = append(actions,rsmServer.RequestVoteRPC(rsmServer.peerIds))

	case "leader":
		timer := SetTimer{rsmServer.heartbeatTimeoutPeriod}
		actions = append(actions,timer)

		actions = append(actions, rsmServer.RequestAppendEntriesRPC(rsmServer.peerIds))
	}

	return actions
}


