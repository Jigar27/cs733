package main
//
import (
	"github.com/cs733-iitb/cluster"
	//"github.com/cs733-iitb/cluster/mock"	
	"github.com/cs733-iitb/log"
	"fmt"
	"os"
	"time"
	"strconv"
	"sync"
	"encoding/gob"
	"errors"
	"bufio"
)
//
type Config struct {
	cluster []NetConfig
	Id int    // selfid
	LogDir string
	ElectionTimeout int64
	HeartbeatTimeout int64
}


//
type NetConfig struct {
	Id int
	Host string
	Port int
}

//type CommitInfo {
//	Data []byte
//	Index int
//	Err error
//}
//
type CommitInfo CommitAction  // leaderID, commitIndex, committedEntry, errorMsg
//
type StateInfo StateStoreAction // term, votedFor, lastMatchedIndex

//
type RaftNode struct {
	sync.Mutex
	active bool
	timer *time.Timer
	logFile *log.Log
	stateFile stateIO   // object of type interface stateIO should implement 	readStateInfo() (StateStoreAction, error), writeStateInfo(StateStoreAction) error, close() error
	logFileDir string   // file dir
	server cluster.Server

	AppendCh chan AppendData
	CommitCh chan CommitInfo
	QuitCh chan int
	ServerStateMachine RaftStateMachineServer
}
//
type Node interface {
	//Mock
	Append([]byte) 
	CommitChannel() (<- chan CommitInfo)
	CommittedIndex() (int64) // baki
	Get(int64) (LogItems, error)
	Id() (int)
	LeaderId() (int)
	ShutDown()
}

//######################################################################## STATEIO   ##########################
type StateStore struct {
	fileName string	
}

type stateIO interface {
	readStateInfo() (StateStoreAction, error)
	writeStateInfo(StateStoreAction) error
	close() error
}

func newStateIO(fileName string) (stateIO, error) {
	var stateStoreObj StateStore
	var file *os.File
	var err error

	stateStoreObj.fileName = fileName
	file, err = os.Open(stateStoreObj.fileName)
	if err != nil {
		file, err = os.Create(stateStoreObj.fileName)
	}
	file.Close()
	return &stateStoreObj, err
}



func (stateStoreObj *StateStore) readStateInfo() (StateStoreAction, error){
	var file *os.File
	var scanner *bufio.Scanner
	var stateStore StateStoreAction
	var err error
	var info []string

	file, err = os.Open(stateStoreObj.fileName)
	if err != nil {
		return stateStore, err
	}

	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		info = append(info, scanner.Text())	
	}		
    file.Close()
    if len(info) < 3 {
    	return stateStore, errors.New("FILE_IS_EMPTY")
    } 
	stateStore.Term, _ = strconv.Atoi(info[0])
	stateStore.VotedFor, _ = strconv.Atoi(info[1])
	stateStore.LastMatchedIndex, _= strconv.ParseInt(info[2], 10, 64)

	return stateStore, scanner.Err()
}

func (stateStoreObj *StateStore) writeStateInfo(stateStore StateStoreAction) error {
	var err error
	var file *os.File
	var writer *bufio.Writer

	file, err = os.Create(stateStoreObj.fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	writer = bufio.NewWriter(file)
	fmt.Fprintln(writer, stateStore.Term)
	fmt.Fprintln(writer, stateStore.VotedFor)
	fmt.Fprintln(writer, stateStore.LastMatchedIndex)
	return writer.Flush()
}

func (stateStoreObj *StateStore) close() error {
	return os.Remove(stateStoreObj.fileName)
}
//###########################################################################################################################

func getClusterConfig(config *Config) cluster.Config {
	// config Config -> cluster []netconfig, id , logdir, electiontimeout , heartbeattimeout
	// cluster []netconfig -> Id, Host, Port
	var (
		id int
	 	address string
	 	peers []cluster.PeerConfig
	 	configFile cluster.Config
	 )

	inboxSize := 1000
	outboxSize := 1000

	for i:=0; i<len(config.cluster);i++{
		id = config.cluster[i].Id
		address = config.cluster[i].Host + ":" + strconv.Itoa(config.cluster[i].Port)
		peers = append(peers, cluster.PeerConfig{id,address})
	}

	configFile = cluster.Config{peers,inboxSize,outboxSize}
	return configFile
}

//
func getPeerIds(config *Config) []int {
	var id int
	var peerIds []int

	for i:=0;i<len(config.cluster);i++ {
		id = config.cluster[i].Id
		if id != config.Id {
			peerIds = append(peerIds, id)
		}
	}

	return peerIds
}


//
func initializeVoteReceived(peerIds []int) map[int]int{
	var voteReceived map[int]int
	voteReceived = make(map[int]int)
	for i:=0;i<len(peerIds);i++ {
		voteReceived[peerIds[i]] = 0
	}

	return voteReceived
}


//
func initializeNextIndex(peerIDs []int) map[int]int64 {
	var nextIndex map[int]int64
	nextIndex = make(map[int]int64)

	for i:=0;i<len(peerIDs);i++ {
		nextIndex[peerIDs[i]] = -1
	}

	return nextIndex
}


//
func initializeMatchIndex(peerIDs []int) map[int]int64 {
	var matchIndex map[int]int64
	matchIndex = make(map[int]int64)

	for i:=0;i<len(peerIDs);i++ {
		matchIndex[peerIDs[i]] = -1
	}

	return matchIndex
}
//.

//
func removeAll(dir string) {
	os.RemoveAll(dir)
}

//
func (raftnode *RaftNode) Append(data []byte) {
	raftnode.AppendCh <- AppendData{data}
}


//
func (raftnode *RaftNode) CommitChannel() (<- chan CommitInfo) {
	return raftnode.CommitCh
}

//
func (raftnode *RaftNode) CommittedIndex() int64 {
	raftnode.Lock()
	defer raftnode.Unlock()

	return raftnode.ServerStateMachine.CommitIndex
}



//
/*func (raftnode *RaftNode) Get(index int64) (LogItems,bool) {
	var result interface{}
	var err error
	var logItem LogItems
	raftnode.Lock()
	defer raftnode.Unlock()

	if raftnode.active {
		//logitem, _ := raftnode.logFile.Get(index)
		result, err = raftnode.logFile.Get(index)
		logItem := result.(LogItems)
		return logItem, true
	} else {
		return LogItems{}, false
	} 
}
*/
func (raftnode *RaftNode) Get(index int64) (logItem LogItems, err error){
	
	raftnode.Lock()
	defer raftnode.Unlock()

	if int(index) < len(raftnode.ServerStateMachine.log) {
		logItem = raftnode.ServerStateMachine.log[index]
		return logItem, nil
	} else {
		err = errors.New("ERROR_OUT_OF_RANGE_ACCESS_TO_LOG")
		return logItem, err
	}
}


//
func (raftnode *RaftNode) Id() (int) {
	raftnode.Lock()
	defer raftnode.Unlock()
	return raftnode.ServerStateMachine.id	
}


//
func (raftnode *RaftNode) LeaderId() (int) {
	raftnode.Lock()
	defer raftnode.Unlock()

	return raftnode.ServerStateMachine.leaderId
}

//
func (raftnode *RaftNode) ShutDown() {
	
	close(raftnode.QuitCh)
}

// you can't just send unknown structures over the clusters. 

func getRegisters() {
	gob.Register(RequestAppendEntries{})
	gob.Register(ResponseAppendEntries{})
	gob.Register(RequestVote{})
	gob.Register(ResponseVote{})
	gob.Register(SendAction{})
	gob.Register(StateInfo{})
	gob.Register(LogItems{})
}
//.

//run the raftnode
//
func (raftnode *RaftNode) startserver() {
	var event Event // to create Event objects from Append()
	var envelope *cluster.Envelope  //To send a message, put it in an “Envelope”
	var result bool
	var actions []Action

	for {
		
		select {
		case event= <- raftnode.AppendCh:
			
			//raftnode.handleEvent(event)
		case <- raftnode.timer.C:
			event = Timeout{}
			//raftnode.handleEvent(event)
		case envelope, _ = <-raftnode.server.Inbox():
			event = envelope.Msg
			//raftnode.handleEvent(event)
		case _, result = <- raftnode.QuitCh:
			if result == false {
				break
			}
		}
		raftnode.Lock()
		actions = raftnode.ServerStateMachine.HandleEvents(event)
		raftnode.Unlock()

		raftnode.handleActions(actions)
	}

	raftnode.timer.Stop()
	raftnode.server.Close()
	close(raftnode.AppendCh)
	close(raftnode.CommitCh)
	raftnode.logFile.Close()
	raftnode.stateFile.close()
	removeAll(raftnode.logFileDir)
}


func (raftnode *RaftNode) handleActions (actions []Action) {
	var err error
	var result bool
	var action Action
	var send SendAction
	var commit CommitAction
	var alarm SetTimer
	var logStore LogStoreAction
	var stateStore StateStoreAction



	for i:=0; i<len(actions); i++ {
		action = actions[i]
		switch action.(type) {
		case SendAction:
			send = action.(SendAction)
			raftnode.server.Outbox() <- &cluster.Envelope{Pid: send.IntendedReceiverID, MsgId: -1, Msg: send.Event}
		
		case CommitAction:
			commit = action.(CommitAction)
			raftnode.CommitCh <- CommitInfo(commit)

		case SetTimer:
			alarm = action.(SetTimer)
			result = raftnode.timer.Reset(time.Duration(alarm.Timer) * time.Millisecond)	
			if !result {
				raftnode.timer = time.NewTimer(time.Duration(alarm.Timer) * time.Millisecond)
			}

		case LogStoreAction:
			logStore = action.(LogStoreAction)
			err = raftnode.logFile.TruncateToEnd(logStore.Index)
			err = raftnode.logFile.Append(logStore.Entry)
			if err != nil {
				
			}



		case StateStoreAction:
			stateStore = action.(StateStoreAction)
			err = raftnode.stateFile.writeStateInfo(stateStore)
		}

	}
}



func (raftnode *RaftNode) getLog() ([]LogItems, error) {
	var log []LogItems
	var logItem LogItems
	var iFace interface{}
	var err error
	var lastIndex int64
	var i int64
	lastIndex = raftnode.logFile.GetLastIndex()
	for i=0; i<=lastIndex; i++ {
		iFace, err = raftnode.logFile.Get(i)
		if err != nil {
			return []LogItems{}, err
		}
		logItem = iFace.(LogItems)
		log = append(log,logItem)
	}
	return log, nil
}

func (raftnode *RaftNode) getState() (StateInfo, error) {
	var rsmStateStore StateStoreAction
	var err error

	rsmStateStore, err = raftnode.stateFile.readStateInfo()

	if err != nil && err.Error() == "ERR_NOT_SET" {
		err = nil
		rsmStateStore.Term = 0
		rsmStateStore.VotedFor = -1
		rsmStateStore.LastMatchedIndex = -1
	}
	return StateInfo(rsmStateStore), err
	
}

//
func New(config *Config) Node {
	var (
		raftnode RaftNode // RaftNode object
		peerIDs []int // for the state machine 
		voteReceived map[int]int // for the state machine 
		nextIndex map[int]int64 // for the state machine 
		matchIndex map[int]int64 // for the state machine 
		clusterConfig cluster.Config // configuration of all servers in the cluster
		err error
		rsmLog []LogItems
		rsmState StateInfo
		rsmLastLogIndex int64
		rsmLastLogTerm int
	) 
	


	raftnode.active = true
	raftnode.timer = time.NewTimer(time.Duration(Random(config.ElectionTimeout, 2 * config.ElectionTimeout)) * time.Millisecond)
	raftnode.AppendCh = make(chan AppendData, 1000)
	raftnode.CommitCh = make(chan CommitInfo, 1000)
	raftnode.QuitCh = make(chan int)

	clusterConfig = getClusterConfig(config)
	peerIDs = getPeerIds(config)
	voteReceived = initializeVoteReceived(peerIDs) 
	nextIndex = initializeNextIndex(peerIDs)
	matchIndex = initializeMatchIndex(peerIDs)
	

	raftnode.logFileDir = config.LogDir

	getRegisters()

	raftnode.server, err = cluster.New(config.Id,clusterConfig)
	//removeAll(raftnode.logFileDir)
	if err != nil {
		fmt.Println("ERROR_NEW(config Config)_cluster.New")
	}


	raftnode.logFile, err = log.Open(raftnode.logFileDir + "/log")
	if err != nil {
		fmt.Println("ERROR_NEW(config Config)_log.open")
	}

	rsmLog, err = raftnode.getLog();
	//raftnode.stateFile, err = log.Open(raftnode.logFileDir + "/state")
	if err != nil {
		fmt.Println("ERROR_NEW(config Config)_raftnode.getLog()")
	}
	



	raftnode.stateFile, err = newStateIO(raftnode.logFileDir + "/state")

	rsmState, err = raftnode.getState();
	
	rsmLastLogIndex = rsmState.LastMatchedIndex
	if rsmLastLogIndex == -1 {
		rsmLastLogTerm = 0
	} else {
		rsmLastLogTerm = rsmLog[rsmLastLogIndex].Term
	}

	//initialize the state machine 
	//raftnode.ServerStateMachine.init(config.Id,-1,-1,0,peerIDs,"follower",raftnode.active,config.ElectionTimeout,config.HeartbeatTimeout,rsmLog,-1,-1,0,rsmState.lastMatchedIndex,voteReceived,nextIndex,matchIndex)

	raftnode.ServerStateMachine.init(rsmState.Term,rsmState.VotedFor,rsmLog, config.Id, peerIDs, 
		config.ElectionTimeout, config.HeartbeatTimeout, rsmState.LastMatchedIndex, "follower", -1, -1, 
		rsmLastLogIndex, rsmLastLogTerm, voteReceived, nextIndex, matchIndex)

	go raftnode.startserver()
	return &raftnode
}
//

/*func main() {

}*/