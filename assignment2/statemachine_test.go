package main 
import (
	"testing"
	//"errors"
	"fmt"
)





func expect(a string, b string) bool {
	if a != b {
		fmt.Println("Expected %v, found %v", b, a) // t.Error is visible when running `go test -verbose`
		return false
	}
	return true
}

func TestAppendDataFollower(t *testing.T){
	var server *Server
	server = &Server{} 
	var actions []Action
	var commandName []byte
	server.serverID = 1
	server.leaderID = 5
	server.votedFor = -1
	server.term = 3
	server.peerIDs = []int{2, 3, 4, 5}
	server.state = "follower"
	server.active = true
	server.timer = float64(heartbeatTimeout)
	server.log = []LogItems{{1,[]byte("x")},{2,[]byte("y")},{3,[]byte("z")}}
	server.commitIndex = 2
	server.lastLogIndex = 3
	server.lastLogTerm = 3
	server.lastMatchedIndex = 3

	data := appendData{commandName}
	actions = server.handleEvents(data)

	if len(actions) != 0 {
		//t.Error("TestAppendData has received some actions")
		sendLI := actions[0].(sendLeaderInfoAction)
		
		switch actions[0].(type) {
		case sendLeaderInfoAction:
			 if !expect(sendLI.msg,"CONTACT_LEADER") {
			 	t.Error("wrong message sent to client")
			 } else {
			 	//fmt.Println("TestAppendDataFollower is ok .....")
			 }
		//		fmt.Println("TestAppendData is OK....")
			

		default :
			t.Error("problem in type..")
		}

	
	}

}

func TestAppendDataCandidate(t *testing.T){
	var server *Server
	server = &Server{} 
	var actions []Action
	var commandName []byte
	server.serverID = 1
	server.leaderID = 5
	server.votedFor = -1
	server.term = 3
	server.peerIDs = []int{2, 3, 4, 5}
	server.state = "candidate"
	server.active = true
	server.timer = float64(electionTimeout)
	server.log = []LogItems{{1,[]byte("x")},{2,[]byte("y")},{3,[]byte("z")}}
	server.commitIndex = 2
	server.lastLogIndex = 3
	server.lastLogTerm = 3
	server.lastMatchedIndex = 3

	data := appendData{commandName}
	actions = server.handleEvents(data)

	if len(actions) != 0 {
		//t.Error("TestAppendData has received some actions")
		sendLI := actions[0].(sendLeaderInfoAction)
		
		switch actions[0].(type) {
		case sendLeaderInfoAction:
			 if !expect(sendLI.msg,"NO_LEADER_EXISTS") {
			 	t.Error("wrong message sent to client")
			 } else {
			 	fmt.Println("TestAppendDataCandidate is ok .....")
			 }
		
		default:
			t.Error("problem in type..")
		}

	}

}


func TestAppendDataLeader(t *testing.T){
	var server *Server
	server = &Server{} 
	var actions []Action
	type commandName []byte
	server.serverID = 1
	server.leaderID = 1
	server.votedFor = 1
	server.term = 2
	server.peerIDs = []int{2, 3, 4, 5}
	server.state = "leader"
	server.active = true
	server.timer = float64(heartbeatTimeout)
	server.log = []LogItems{{term: 1}, {term: 2}, {term: 2}, {term: 2}}
	server.commitIndex = 3
	server.lastLogIndex = 3
	server.lastLogTerm = 2
	server.lastMatchedIndex = 3
	server.voteReceived = map[int]int{2:1, 3:1, 4:1, 5:-1}
	server.nextIndex = map[int]int{2:4, 3:4, 4:4, 5:4}
	server.matchIndex = map[int]int{2:3, 3:3, 4:3, 5:3}
	data := appendData{[]byte("w")}
	actions = server.handleEvents(data)

	//fmt.Println(len(actions))
	/*if len(actions) != 5 {
		//one is logstore action and other four are send appendEntriesRPC actions
		t.Error("TestAppendDataLeader: length of actions is not 5")
	} */
	logStore := actions[0].(LogStoreAction)
	switch actions[0].(type) {
	case LogStoreAction :
		//logStore := actions[0].(LogStoreAction)
		if logStore.index != 4 && logStore.entry.term != 2 {
			t.Error("TestAppendDataLeader : last log index or term error")
		}
	default:
		t.Error("TestAppendDataLeader: unknown type")
	}

	switch actions[1].(type) {
	case sendAction :
		send := actions[1].(sendAction)
		switch send.event.(type) {
		case requestAppendEntries :
			fmt.Println("Event requestAppendEntries is fired")
			fmt.Println("TestAppendDataLeader is ok ..")
		default:
			t.Error("Error in Event")
		}
	}
}

func TestTimeoutFollower(t *testing.T) {
	var server *Server
	server = &Server{} 
	var actions []Action
	
	server.serverID = 3
	server.leaderID = 5
	server.votedFor = -1
	server.term = 3
	server.peerIDs = []int{1, 2, 4, 5}
	server.state = "follower"
	server.active = true
	server.timer = float64(heartbeatTimeout)
	server.log = []LogItems{{1,[]byte("x")},{2,[]byte("y")},{3,[]byte("z")}}
	server.commitIndex = 2
	server.lastLogIndex = 3
	server.lastLogTerm = 3
	server.lastMatchedIndex = 3


	alarm := timeout{}
	actions = server.handleEvents(alarm)

	if server.state != "candidate" {
		t.Error("TestTimeoutFollowerERROR : server state has not changed")
	}

	if server.votedFor != 3 {
		t.Error("TestTimeoutFollowerERROR: server has not voted for itself")
	} 

	if server.term != 4 {
		t.Error("TestTimeoutFollowerERROR: term has not been updated")
	}

	if(len(actions) != 0) {
		switch actions[0].(type) {
		case setTimer :
			fmt.Println("timer is reset..")
		default:
			t.Error("TestTimeoutFollowerERROR: Timer is not reset")
		}

		switch actions[1].(type) {
		case StateStoreAction :
			fmt.Println("statestore is chanded")
		default :
			t.Error("TestTimeoutFollowerERROR: statestore")
		}

	} else {
		t.Error("no actions returned")
	}
}

func TestTimeoutCandidate(t *testing.T) {
	var server *Server
	server = &Server{} 
	var actions []Action
	
	server.serverID = 1
	server.leaderID = 5
	server.votedFor = -1
	server.term = 3
	server.peerIDs = []int{2, 3, 4, 5}
	server.state = "candidate"
	server.active = true
	server.timer = float64(electionTimeout)
	server.log = []LogItems{{1,[]byte("x")},{2,[]byte("y")},{3,[]byte("z")}}
	server.commitIndex = 2
	server.lastLogIndex = 3
	server.lastLogTerm = 3
	server.lastMatchedIndex = 3

	alarm := timeout{}
	actions = server.handleEvents(alarm)

	if server.votedFor != 1 {
		t.Error("TestTimeoutCandidateERROR: server has not voted for itself")
	} 

	if server.term != 4 {
		t.Error("TestTimeoutCandidateERROR: term has not been updated")
	}

	if(len(actions) != 0) {
		switch actions[0].(type) {
		case setTimer :
			fmt.Println("timer is reset..")
		default:
			t.Error("TestTimeoutCandidateERROR: Timer is not reset")
		}

		switch actions[1].(type) {
		case StateStoreAction :
			fmt.Println("statestore is chanded")
		default :
			t.Error("TestTimeoutCandidateERROR: statestore")
		}

	} else {
		t.Error("no actions returned")
	}
}

func TestTimeoutLeader(t *testing.T) {
	var server *Server
	server = &Server{} 
	var actions []Action
	
	server.serverID = 1
	server.leaderID = 1
	server.votedFor = 1
	server.term = 2
	server.peerIDs = []int{2, 3, 4, 5}
	server.state = "leader"
	server.active = true
	server.timer = float64(heartbeatTimeout)
	server.log = []LogItems{{term: 1}, {term: 2}, {term: 2}, {term: 2}}
	server.commitIndex = 3
	server.lastLogIndex = 3
	server.lastLogTerm = 2
	server.lastMatchedIndex = 3
	server.voteReceived = map[int]int{2:1, 3:1, 4:1, 5:-1}
	server.nextIndex = map[int]int{2:4, 3:4, 4:4, 5:4}
	server.matchIndex = map[int]int{2:3, 3:3, 4:3, 5:3}

	alarm := timeout{}
	actions = server.handleEvents(alarm)

	if(len(actions) != 0) {
		switch actions[0].(type) {
		case setTimer :
			fmt.Println("timer is reset..")
		default:
			t.Error("TestTimeoutLeaderERROR: Timer is not reset")
		}

		
		switch actions[1].(type) {
		case sendAction:
			send := actions[1].(sendAction)
			switch send.event.(type) {
			case requestAppendEntries:
			default:
				t.Error("TestTimeoutLeaderERROR: AppendEntriesReqEvent")
			}
		}

		
	}
}
