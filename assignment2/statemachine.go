package main

import (
		
		"time"
		
		"math/rand"
)




const (
	
	heartbeatTimeout = 100 * time.Millisecond 
	electionTimeout = 2 * heartbeatTimeout
	
)

type Event interface{}

type Action interface{}

type Timeout struct {

}



type setTimer struct {
	timer float64
}

type appendData struct	{
	commandName []byte
	//uniqueId int // this id is given by the clients 
}



type LogItems struct {
	term int
	commandName []byte
	//uniqueId uint64  // 
	//commited bool
}



type requestAppendEntries struct {
	senderID int	
	senderTerm int
	prevLogIndex int
	prevLogTerm int
	
	//leaderId uint64
	logEntries []LogItems
	commitIndex int
}

type responseAppendEntries struct {
	senderID int	
	senderTerm int 	
	senderLastMatchedIndex int
	success bool
	
}

type requestVote struct {
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
	
}

type responseVote struct {
	senderID int
	senderTerm int
	success bool 
}

type timeout struct{

}

type sendAction struct {
	intendedReceiverID int
	event Event
}


type LogStoreAction struct {
	index int
	entry LogItems
}

type StateStoreAction struct {
	term int
	votedFor int
	lastMatchedIndex int
}

type CommitAction struct {
	leaderID int
	commitIndex int
	committedEntry LogItems
	//err 
}

type sendLeaderInfoAction struct{
	leaderID int
	msg string
}

type Server struct {
	serverID int
	leaderID int
	votedFor int
	term int
	peerIDs []int
	state string
	active bool
	timer float64
	log []LogItems
	commitIndex int
	lastLogIndex int
	lastLogTerm int
	lastMatchedIndex int
	voteReceived map[int]int // 1-> granted, 0-> not received, -1-> denied
	nextIndex map[int]int
	matchIndex map[int]int
}



/*func newLog() *LogItems {
	l := &LogItems{[]LogItems}
	return l
}*/


func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func(server  *Server) intializeNewServer(id int,leader int,votedFor int, term int, peerIDs []int, state string, active bool,timer float64,log []LogItems,commitIndex int,lastLogIndex int,lastLogTerm int  ,lastMatchedIndex int,voteReceived map[int]int,nextIndex map[int]int,matchIndex map[int]int)  {

	// create log for the server
	//log := newLog()
	//server Server
	server = &Server { id,
						leader,
						votedFor,
						term,
						peerIDs,
						state, 
						active,
						timer,
						log,
						commitIndex,
						lastLogIndex,
						lastLogTerm,
						lastMatchedIndex,
						voteReceived,
						nextIndex,
						matchIndex }

	

}



func(server *Server) requestAppendEntriesRPC(serverIDs []int) []Action {
	var actions []Action
	for i:=0; i<len(serverIDs);i++ {
		prevLogIndex := server.nextIndex[serverIDs[i]]-1
		prevLogTerm := server.log[prevLogIndex].term
		logEntries := server.log[server.nextIndex[serverIDs[i]]:]
		requestAppend := requestAppendEntries {server.serverID,server.term,prevLogIndex,prevLogTerm,logEntries,server.commitIndex}

		send := sendAction{serverIDs[i],requestAppend}
		actions = append(actions, send)   
	}

	return actions
}



func (server *Server) requestVoteRPC(serverIDs []int) []Action {
	var actions []Action
	for i:=0; i< len(serverIDs); i++ {
		reqVote := requestVote{server.term,server.serverID,server.lastLogIndex,server.lastLogTerm}
		send := sendAction{serverIDs[i],reqVote}
		actions = append(actions, send)
	}

	return actions
}



func (server *Server) handleEvents(event Event) []Action{
	var actions []Action
	switch event.(type) {
	case requestAppendEntries:
		actions = server.handleRequestAppendEntries(event)

	case responseAppendEntries:
		actions = server.handleResponseAppendEntries(event)

	case requestVote:
		actions = server.handleRequestVote(event)

	case responseVote:
		actions = server.handleResponseVote(event)

	case appendData:
		actions = server.handleAppendData(event)

	case timeout:
		actions = server.handleTimeout(event)
	}

	return actions
}


func(server *Server) handleRequestAppendEntries(event Event) []Action {
	var actions []Action
	requestAppendEntry,_ := event.(requestAppendEntries)
	senderID := requestAppendEntry.senderID
	senderTerm := requestAppendEntry.senderTerm
	prevLogIndex := requestAppendEntry.prevLogIndex
	prevLogTerm := requestAppendEntry.prevLogTerm
	logEntries := requestAppendEntry.logEntries
	commitIndex := requestAppendEntry.commitIndex

	//act accordingly the state of the server

	switch server.state {
	
		
	case "follower" :
		if server.term > senderTerm {
			//since this server has a higher term then sender server, the sender server might be the old leader
			responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,false}
			send := sendAction{senderID,responseAppendEntry}
			actions = append(actions,send) 
		} else {
			//since this request has come from a legitimate leader it also serves as a heartbeat from tha leader so i need to restart the timer to wait for the next heartbeat
			timer := setTimer{float64(random(int(heartbeatTimeout),int(electionTimeout)))}
			actions = append(actions,timer)
			// the server is not old. if this server's term is older than leader's term then update the state of this server
			if server.term < senderTerm {
				server.term = senderTerm
				server.leaderID = senderID
				server.lastMatchedIndex = -1
			}
			


			//its time for append entries consistency check
			//if this server accepts this append entry then it means that this server's log is matched till this new index with leader's log
			//this server accepts this append entry only when this server's last log index and last log term matches with leader's prev log index and prev log term. 

			if server.log[prevLogIndex].term != prevLogTerm {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,false}
				send := sendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				server.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0 { // this means that this append request rpc contains data. not just a heartbeat signal.
					server.lastLogIndex = prevLogIndex
					server.log = server.log[:server.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					server.lastLogIndex++
					server.log = append (server.log,logEntries[i])
					server.lastLogTerm = server.log[server.lastLogIndex].term
					server.lastMatchedIndex = server.lastLogIndex
					
					logStore := LogStoreAction{server.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact 
				stateStore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
				actions = append(actions,stateStore)
				if server.commitIndex < commitIndex {
					if server.commitIndex < server.lastLogIndex{
						if commitIndex <= server.lastLogIndex{
							server.commitIndex = commitIndex
						} else{
							server.commitIndex = server.lastLogIndex
						}
						cm := CommitAction{server.leaderID,server.commitIndex,server.log[server.commitIndex]}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,true}
				send := sendAction {senderID,responseAppendEntry}
				actions = append(actions,send)
			}
		}

	case "candidate" :
		if server.term > senderTerm {
			responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,false}
			send := sendAction{senderID,responseAppendEntry}
			actions = append(actions,send) 
		} else {
			//since this request has come from a legitimate leader it also serves as a heartbeat from tha leader so i need to restart the timer to wait for the next heartbeat
			//plus this server is in candidate state so it has to revert back its state to follower
			server.state = "follower"
			timer := setTimer{float64(random(int(heartbeatTimeout),int(electionTimeout)))}
			actions = append(actions,timer)

			server.term = senderTerm
			server.leaderID = senderID
			server.lastMatchedIndex = -1


			//its time for append entries consistency check
			//if this server accepts this append entry then it means that this server's log is matched till this new index with leader's log
			//this server accepts this append entry only when this server's last log index and last log term matches with leader's prev log index and prev log term. 

			if server.log[prevLogIndex].term != prevLogTerm {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,false}
				send := sendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				server.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0  { // this means that this append request rpc contains data. not just a heartbeat signal.
					server.lastLogIndex = prevLogIndex
					server.log = server.log[:server.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					server.log = append (server.log,logEntries[i])
					server.lastLogIndex++
					server.lastLogTerm = server.log[server.lastLogIndex].term
					server.lastMatchedIndex = server.lastLogIndex
					//store this action
					logStore := LogStoreAction{server.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact in statestore
				stateStore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
				actions = append(actions,stateStore)
				if server.commitIndex < commitIndex {
					if server.commitIndex < server.lastLogIndex{
						if commitIndex <= server.lastLogIndex{
							server.commitIndex = commitIndex
						} else{
							server.commitIndex = server.lastLogIndex
						}
						cm := CommitAction{server.leaderID,server.commitIndex,server.log[server.commitIndex]}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,true}
				send := sendAction{senderID,responseAppendEntry}
				actions = append(actions,send)
			}

		}

	case "leader" :
		if server.term > senderTerm {
			responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,false}
			send := sendAction{senderID,responseAppendEntry}
			actions = append(actions,send) 
		} else {
			//since this request has come from a legitimate leader it also serves as a heartbeat from tha leader so i need to restart the timer to wait for the next heartbeat
			//plus this server is in candidate state so it has to revert back its state to follower
			server.state = "follower"
			timer := setTimer{float64(random(int(heartbeatTimeout),int(electionTimeout)))}
			actions = append(actions,timer)

			server.term = senderTerm
			server.leaderID = senderID
			server.lastMatchedIndex = -1


			//its time for append entries consistency check
			//if this server accepts this append entry then it means that this server's log is matched till this new index with leader's log
			//this server accepts this append entry only when this server's last log index and last log term matches with leader's prev log index and prev log term. 

			if server.log[prevLogIndex].term != prevLogTerm {
				//this means at prevLogIndex there is a mismatch with leader's log
				//this server should send lastMatchedIndex to tell the leader that this is the maximum index where match happens with you.
				responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,false}
				send := sendAction{senderID,responseAppendEntry}
				actions = append(actions,send) 
			} else {
				//it is the time to append all entries in this server's log
				//at this moment this server log matches with leader's log till prevLogIndex
				server.lastMatchedIndex = prevLogIndex
				//this server may have extraneous entries that is given by other leader and which had not been committed. we have to discard this entries
				if len(logEntries) > 0 { // this means that this append request rpc contains data. not just a heartbeat signal.
					server.lastLogIndex = prevLogIndex
					server.log = server.log[:server.lastLogIndex+1]
				}//				

				//put entries one by one into this server's log 

				for i:=0; i<len(logEntries); i++ {
					server.log = append (server.log,logEntries[i])
					server.lastLogIndex++
					server.lastLogTerm = server.log[server.lastLogIndex].term
					server.lastMatchedIndex = server.lastLogIndex
					//store this action
					logStore := LogStoreAction{server.lastLogIndex,logEntries[i]}
					actions = append(actions,logStore)
				}
				// since log is changed server's state is changed. reflect this fact in statestore
				stateStore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
				actions = append(actions,stateStore)
				if server.commitIndex < commitIndex {
					if server.commitIndex < server.lastLogIndex{
						if commitIndex <= server.lastLogIndex{
							server.commitIndex = commitIndex
						} else{
							server.commitIndex = server.lastLogIndex
						}
						cm := CommitAction{server.leaderID,server.commitIndex,server.log[server.commitIndex]}
						actions = append(actions,cm)
					}
				}

				//now you have processed the append request entries rpc, so you have to send append response to leader.
				responseAppendEntry := responseAppendEntries{server.serverID,server.term,server.lastMatchedIndex,true}
				send := sendAction {senderID,responseAppendEntry}
				actions = append(actions,send)
			}

		}


	}

	return actions
}


func(server *Server) handleResponseAppendEntries(event Event) []Action{
	var actions []Action
	responseAppendEntry,_ := event.(responseAppendEntries)
	senderID := responseAppendEntry.senderID
	senderTerm := responseAppendEntry.senderTerm
	senderLastMatchedIndex := responseAppendEntry.senderLastMatchedIndex
	success := responseAppendEntry.success

	switch server.state{
	case "follower":
			//follower can't process this RPC
	case "candidate":
			//candidate can't process this RPC
	case "leader":
		//if leader is old leader then ..
		if server.term < senderTerm {
			server.state = "follower"
			server.term = senderTerm
			server.votedFor = -1
			server.lastMatchedIndex = -1
			stateStore := StateStoreAction{server.term, server.votedFor, server.lastMatchedIndex}
			actions = append(actions, stateStore)
		} else if success == true {
			server.matchIndex[senderID] = senderLastMatchedIndex
			server.nextIndex[senderID] = server.matchIndex[senderID] + 1
			noCopy := 1 // since leader has already an original copy of that.
			for i := server.matchIndex[senderID] ; i > server.commitIndex ; i-- {
				for j:=0; j<len(server.peerIDs); j++ {
					if server.matchIndex[j] >= i {
						noCopy ++
						if noCopy > (len(server.peerIDs)+1)/2 {
							server.commitIndex = i 
							cm := CommitAction{server.serverID,server.commitIndex,server.log[server.commitIndex]}
							actions = append(actions,cm)
							break
						}
					}
				}
				noCopy = 1
			}


		} else {
			server.nextIndex[senderID]--
			// should call requestappendentriesRPC with this new nextindex
			actions = append(actions,server.requestAppendEntriesRPC([]int{senderID}))
		}


	}

	return actions
}


func(server *Server) handleRequestVote(event Event) []Action{
	var actions []Action
	requestVote := event.(requestVote)
	candidateTerm := requestVote.term
	candidateId := requestVote.candidateId
	candidateLastLogIndex := requestVote.lastLogIndex
	candidateLastLogTerm := requestVote.lastLogTerm

	switch server.state{
	case "follower":
		// if you receive a vote request from a candidate whose term is higher than you then it is the time to change the term
		if server.term < candidateTerm {
			server.term = candidateTerm
			//since this is newer term this server has not voted to anyone in this newer term
			server.votedFor = -1
			//newer term may have new leader altogether so this server has not matched anything with newer leader yet
			server.lastMatchedIndex = -1
			statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
			actions = append(actions,statestore)
		}

		//check whether the candidate is as modern and knowledgeble as you 
		if server.term > candidateTerm { 
			resVote := responseVote{server.serverID,server.term,false}
			send := sendAction{candidateId,resVote}
			actions = append(actions,send)
		} else if server.lastLogTerm > candidateLastLogTerm{
			resVote := responseVote{server.serverID,server.term,false}
			send := sendAction{candidateId,resVote}
			actions = append(actions,send)
		} else if server.lastLogTerm == candidateLastLogTerm && server.lastLogIndex > candidateLastLogIndex {
			resVote := responseVote{server.serverID,server.term,false}
			send := sendAction{candidateId,resVote}
			actions = append(actions,send)
		} else if server.votedFor != -1 && server.votedFor != server.serverID { // if server has voted to anyone or server itself is a candidate then deny vote
			resVote := responseVote{server.serverID,server.term,false}
			send := sendAction{candidateId,resVote}
			actions = append(actions,send)
		} else { // vote this candidate
			resVote := responseVote{server.serverID,server.term,true}
			send := sendAction{candidateId,resVote}
			actions = append(actions,send)

			// server state is changed
			server.votedFor = candidateId
			statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
			actions = append(actions,statestore)
		}

	case "candidate":
		if server.term < candidateTerm {
			server.state = "follower"
			server.term = candidateTerm
			server.votedFor = -1
			server.lastMatchedIndex = -1
			statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
			actions := append(actions,statestore)

			////check whether the candidate is as modern and knowledgeble as you 
			if server.lastLogTerm > candidateLastLogTerm {
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if server.lastLogTerm == candidateLastLogTerm && server.lastLogIndex > candidateLastLogIndex {
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if server.votedFor != -1 && server.votedFor != server.serverID { // if server has voted to anyone or server itself is a candidate then deny vote
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
			} else{
				resVote := responseVote{server.serverID,server.term,true}
				send := sendAction{candidateId,resVote}
				actions := append(actions,send)

				// server state is changed
				server.votedFor = candidateId
				statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
				actions = append(actions,statestore)
			}
		} else if server.term == candidateTerm{
			// since this server is in candidate state and its term is actully the current term of the system it can't revote 
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
		} else{
			// in this case , this server is modern than the candidate server so deny the vote
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
		}

	case "leader":
		if server.term < candidateTerm {
			server.state = "follower"
			server.term = candidateTerm
			server.votedFor = -1
			server.lastMatchedIndex = -1
			statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
			actions := append(actions,statestore)

			////check whether the candidate is as modern and knowledgeble as you 
			if server.lastLogTerm > candidateLastLogTerm {
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if server.lastLogTerm == candidateLastLogTerm && server.lastLogIndex > candidateLastLogIndex {
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
			} else if server.votedFor != -1 && server.votedFor != server.serverID { // if server has voted to anyone or server itself is a candidate then deny vote
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
			} else{
				resVote := responseVote{server.serverID,server.term,true}
				send := sendAction{candidateId,resVote}
				actions := append(actions,send)

				// server state is changed
				server.votedFor = candidateId
				statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
				actions = append(actions,statestore)
			}
		} else if server.term == candidateTerm{
			//candidate's term is the same as this server's term.
			//to ensure the safety property "every term consists of atmost one leader" this server as being the leader should deny the candidate
			resVote := responseVote{server.serverID,server.term,false}
			send := sendAction{candidateId,resVote}
			actions = append(actions,send)
		} else{
			// in this case , this server is modern than the candidate server so deny the vote
				resVote := responseVote{server.serverID,server.term,false}
				send := sendAction{candidateId,resVote}
				actions = append(actions,send)
		}
	}

	return actions
}

func(server *Server) handleResponseVote(event Event) []Action{
	var actions []Action
	var noVoteGranted,noVoteDenied int
	resVote := event.(responseVote)
	senderID := resVote.senderID
	senderTerm := resVote.senderTerm
	senderResponse := resVote.success

	switch server.state {
	case "follower":
		if server.term < senderTerm {
			server.term = senderTerm
			server.votedFor = -1
			server.lastMatchedIndex = -1
			statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
			actions = append (actions,statestore)
		}

	case "candidate":
		//we have to maintain 2 variables in order to decide whether this candidate server is liable to become a server or not 
		noVoteGranted = 1 // since server votes for itself
		noVoteDenied = 0

		for i:=0; i<len(server.peerIDs);i++ {
			if server.voteReceived[i] == 1 {
				noVoteGranted++
				continue
			}
			if server.voteReceived[i] == -1 {
				noVoteDenied++
				continue
			}
		}

		if server.term < senderTerm {
			server.state = "follower"
			server.term = senderTerm
			server.votedFor = -1
			server.lastMatchedIndex = -1
			statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
			actions = append (actions,statestore)
		} else if senderResponse == true {
			// now check whether this server has got enough vote to become leader
			server.voteReceived[senderID] = 1
			noVoteGranted++
			if noVoteGranted > (len(server.peerIDs)+1)/2 {
				//now this server can become leader of the system
				server.state = "leader"
				server.leaderID = server.serverID
				// now this server has to maintain nextindex and last match index for each follower
				server.nextIndex = make(map[int]int)
				server.matchIndex = make(map[int]int)
				//initialize next index value for each follower equal to this server lastLogIndex+1
				//initialize match index value for each follower equal to -1
				for i:=0;i<len(server.peerIDs);i++ {
					server.nextIndex[i]=server.lastLogIndex+1
					server.matchIndex[i]= -1
				}
				// next thing this server has to do is replicate its log onto each follower

				actions = append(actions,server.requestAppendEntriesRPC(server.peerIDs))
			}
		} else if senderResponse == false {
			//reflect this fact 
			server.voteReceived[senderID] = -1
			noVoteDenied++
		}

		//noVoteReceived := noVoteDenied + noVoteGranted
		//what happened when timeout occu

	case "leader":
		if server.term < senderTerm {
			server.state = "follower"
			server.term = senderTerm
			server.votedFor = -1
			server.lastMatchedIndex = -1
			statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
			actions = append (actions,statestore)
		}
	}

	return actions
}


func(server *Server) handleAppendData(event Event) []Action {
	var actions []Action
	var commandName []byte
	appendDataST := event.(appendData)
	commandName = appendDataST.commandName

	switch server.state {
	case "follower" :
		//If this server is in follower state then it has nothing to do with this event.
		//If this server knows who is the leader of this system then simply responds with leader information
		sendLI := sendLeaderInfoAction{server.leaderID,"CONTACT_LEADER"}
		actions = append(actions,sendLI)

	case "candidate" :
		//Since this server is in candidate state, from this server's point of view there is no leader exist in the system
		sendLI := sendLeaderInfoAction{server.leaderID,"NO_LEADER_EXISTS"}
		actions = append(actions,sendLI)
	
	case "leader" :
		//leader will make a new entry into its log and store commandName
		server.lastLogIndex++
		//server.log[server.lastLogIndex].term = server.term
		//server.log[server.lastLogIndex].commandName = commandName
		//server.lastLogTerm = server.log[server.lastLogIndex].term
		server.log = append(server.log, LogItems{server.term, commandName})
		server.lastLogTerm = server.log[server.lastLogIndex].term
		logstore := LogStoreAction{server.lastLogIndex,server.log[server.lastLogIndex]}
		actions = append(actions,logstore)

		//after appending its own log, next action that this leader should take is to replicate this entry into its follower's log
		actions = append(actions,server.requestAppendEntriesRPC(server.peerIDs))
	}

	return actions
}


func(server *Server) handleTimeout(event Event) []Action{
	var actions []Action

	//this event tells that previous timeout interval elapsed so restart new interval for timeout
	timer := setTimer{float64(random(int(heartbeatTimeout),int(electionTimeout)))}
	actions = append(actions,timer)

	switch server.state{
	case "follower":
		//since timeout interval has elapsed, first guess of this server is that no leader exists in the system
		server.state = "candidate"
		server.term++
		server.votedFor = server.serverID
		server.lastMatchedIndex = -1
		server.voteReceived = make(map[int]int)
		// without following step you would get error while testing "nil map assignment"
		//for i:= 0; i < len(server.peerIDs); i++ {
		//	server.voteReceived[server.peerIDs[i]] = 0
		//}

		statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
		actions = append(actions,statestore)
		//ask other servers to vote for itself
		actions = append(actions,server.requestVoteRPC(server.peerIDs))

	case "candidate":
		//retry again for the next timeout interval
		server.term++
		server.votedFor = server.serverID
		server.lastMatchedIndex = -1
		//refresh voteReceived
		server.voteReceived = make(map[int]int)
		for i:=0; i<len(server.peerIDs); i++ {
			server.voteReceived[i] = 0
		}

		statestore := StateStoreAction{server.term,server.votedFor,server.lastMatchedIndex}
		actions = append(actions,statestore)
		//ask other servers to vote for itself
		actions = append(actions,server.requestVoteRPC(server.peerIDs))

	case "leader":
		actions = append(actions, server.requestAppendEntriesRPC(server.peerIDs))
	}

	return actions
}

func main(){

}
