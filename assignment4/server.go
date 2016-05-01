
package main

import (
	"bufio"
	"fmt"
	"github.com/Jigar27/cs733/assignment4/fs"
	// "fs"
	"net"
	"os"
	"strconv"
	//"errors"
	"encoding/json"
	"encoding/gob"
	"bytes"
	"sync"
	//"time"
	//"math/rand"
	"os/exec"	
)

type clientMap struct {
	sync.Mutex
	cMap map[int]*net.TCPConn
}


var crlf = []byte{'\r', '\n'}
var id int
var raftnode Node
var clientConnMap clientMap
var host string
var port int

//########################################################### methods for map###############################################################

func (clMap *clientMap) make() {
	clMap.Lock()
	defer clMap.Unlock()
	clMap.cMap = make(map[int]*net.TCPConn)
}


func (clMap *clientMap) add(clientId int, connection *net.TCPConn) {
	clMap.Lock()
	defer clMap.Unlock()
	clMap.cMap[clientId] = connection
}


func (clMap *clientMap) getValue(clientId int) (connection *net.TCPConn, result bool) {
	clMap.Lock()
	defer clMap.Unlock()
	connection, result = clMap.cMap[clientId]
	return connection, result
}


func (clMap *clientMap) remove(clientId int) {
	clMap.Lock()
	clMap.Unlock()
	delete(clMap.cMap,clientId)
}

//###############################################   misc   ###########################################



func getMin(a, b int64) int64 {
	if a <= b {	
		return a
	} 
	return b
}


func getMax(a, b int64) int64 {
	if a >= b {	
		return a
	} 
	return b
}


func execBashCommand(cmdString string) (err error) {
	var (
		cmd *exec.Cmd
	)

	cmd = exec.Command("sh", "-c", cmdString)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	return err
}
//#############################################################################################

//###############################################################################################
func decode(data []byte) (msg *fs.Msg, err error) {
	var buffer *bytes.Buffer
	var	decoder *gob.Decoder
	

	// msg = &fs.Msg{}
	msg = &fs.Msg{}
	buffer = bytes.NewBuffer(data)
	decoder = gob.NewDecoder(buffer)
	err = decoder.Decode(msg)
	return msg, err
}




//###################################################################################################

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func reply(conn *net.TCPConn, msg *fs.Msg) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"		
	case 'R':
		resp = "ERR_REDIRECT " + host + " " + strconv.Itoa(msg.ReDirPort)	
	case 'T':
		resp = "ERR_TRY_LATER"

	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func serve(conn *net.TCPConn, clientId int) {
	var response *fs.Msg
	var msg *fs.Msg
	var reader *bufio.Reader
	
	var	msgerr error
	var	fatalerr error			
	
	var	err error
	var	data []byte
	reader = bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr = fs.GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			//conn.Close()
			closeClientConnection(conn, clientId)
			break
		}

		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				closeClientConnection(conn, clientId)
				break
			}
		}

		if msg.Kind == 'r' {
			response = fs.ProcessMsg(msg)
			if !reply(conn, response) {
				closeClientConnection(conn,clientId)
				break
			}
		} else {
			msg.ClientId =clientId
			data, err = encode(msg)
			if err != nil {
				reply(conn, &fs.Msg{Kind:'I'})
				closeClientConnection(conn, clientId)
				break
			}
			raftnode.Append(data)
		}

		
		/*response := fs.ProcessMsg(msg)
		if !reply(conn, response) {
			conn.Close()
			break
		}*/
	}
}

func startServer() {
	var clientId int
	clientId = 0
	host = "localhost"
	port = 5000 + id
	//
	clientConnMap.make()
	tcpaddr, err := net.ResolveTCPAddr("tcp", host + ":" + strconv.Itoa(port))
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)

	for {
		// mapping clients to map
		clientId := (clientId+1) % 1000  // 1000 is the assumption that at any time this server will server to atmost 1000 clients
		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)

		// once you accept the connection then add clientid and connection handler in the map so that clienthandler will eventually reply to correct client
		clientConnMap.add(clientId,tcp_conn)
		go serve(tcp_conn, clientId)
	}
}


func initializeRaft() {

	var configuration *Config // // this configuration is in Config structure format
	var err error
	var configFile *os.File //file discriptor
	var filename string
	var decoder *json.Decoder
	//var config Config   

	filename = "config/config" + strconv.Itoa(id) + ".json"
	configFile, err = os.Open(filename)
	if err != nil {
		fmt.Println("server.go_initializeRaft_ERR_Opening_json")
	}
	defer configFile.Close()
	//this decoder will read the json file and help to fill up config.Config
	decoder = json.NewDecoder(configFile)
	err = decoder.Decode(&configuration)

	if err != nil {
		fmt.Println("server.go_initializeRaft_ERR_Docoding")
	}

	configuration.Id = id // selfid
	configuration.LogDir = strconv.Itoa(configuration.Id)

	raftnode = New(configuration)

}

func startFileServer() {
	var ch <- chan CommitInfo
	var result bool
	var commitInfo CommitInfo
	var msg *fs.Msg
	var	conn *net.TCPConn
	var i int64
	var leaderPort int
	var logItem LogItems
	var err error
	var response *fs.Msg
	var lastComIndex int64// this value tells the file server that from which log entry index to latest commit index are commited entries 
	lastComIndex = -1
	ch = raftnode.CommitChannel() // use Node interface method to get the channel to listen from the raftnode

	//continuously receive from commit channel

	for {
		commitInfo = <- ch //receive from commit channel
		fmt.Println(id,commitInfo.LeaderID,commitInfo.CommitIndex,commitInfo.ErrorMsg)

		if commitInfo.ErrorMsg != nil {
			msg,err = decode(commitInfo.CommittedEntry.CommandName)
			if err != nil {
				fmt.Println("server.go_startFileServer_ERR_decodingCommandname")
			}

			conn, result = clientConnMap.getValue(msg.ClientId)

			//reply to corresponding client

			switch commitInfo.ErrorMsg.Error(){

			case "ERR_CONTACT_LEADER" :
				leaderPort = 6000 + commitInfo.LeaderID
				reply(conn, &fs.Msg{Kind: 'R', ReDirPort: leaderPort})

			case "ERR_TRY_LATER" :
				reply(conn, &fs.Msg{Kind: 'T'})

			default:
			}
		} else {
			// no ErrorMsg in CommitInfo CommitAction, this means details about commited entries has arrived
			//remember CommitIndex points to last committed entry. in face all entries from lastCommitIndex to CommitIndex are commited entries.

			for i = lastComIndex+1; i<= commitInfo.CommitIndex; i++ {
				// to get the logitems use Get() provided in interface of raftnode

				logItem, err = raftnode.Get(i)
				if err != nil {
					fmt.Println("server.go_startFileServer_ERR_Get(index)")
				}

				msg, err = decode(logItem.CommandName)
				if err != nil {
					fmt.Println("server.go_startFileServer_ERR_decode(logItem.CommandName)")
				}

				response = fs.ProcessMsg(msg)

				conn, result = clientConnMap.getValue(msg.ClientId)
				if result {

				}
				/*if result {
					utils.Assert(ci.LeaderId == selfId, "startfs: clientid found but it is not a leader")
					reply(conn, response)
				}*/

				if commitInfo.LeaderID == id {
					reply(conn,response)
				}			
			}  
			/*if commitInfo.CommitIndex >= lastCommitIndex {
				lastCommitIndex = CommitInfo.CommitIndex
			}*/

			lastComIndex = getMax(lastComIndex,commitInfo.CommitIndex)
		} 
	}

}


func encode(msg *fs.Msg) (data []byte, err error) {
	var encoder *gob.Encoder

	buffer := &bytes.Buffer{}
	encoder = gob.NewEncoder(buffer)
	err = encoder.Encode(msg)
	return buffer.Bytes(), err
}


func closeClientConnection(conn *net.TCPConn, clientId int) {
	conn.Close()
	clientConnMap.remove(clientId)
}

func serverMain(serverId int) {
	id = serverId // global variable
	initializeRaft()
	go startServer()
	startFileServer()
	/*tcpaddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)

	for {
		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)
		go serve(tcp_conn)
	}*/
}

func main() {
	// you call the process by supplying the ID of this server via commandline argument
	var serverId int
	var err error

	if len(os.Args) != 2 {
		fmt.Println("USAGE: server <serverId>")
	}

	serverId, err = strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid serverId")
	}
	serverMain(serverId)
}
