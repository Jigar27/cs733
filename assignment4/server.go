
package main

import (
	"bufio"
	"fmt"
	"github.com/Jigar27/cs733/assignment4/fs"
	"net"
	"os"
	"strconv"
	//"errors"
)

type clientMap struc {
	sync.Mutex
	cMap map[int]*net.TCPConn
}


var crlf = []byte{'\r', '\n'}
var id int
var raftnode Node
var clientConnMap clientMap
var host string
var port int



##########################################################################################
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


func exexBashCommand(cmdString string) (err error) {
	var (
		cmd *exec.Cmd
	)

	cmd = exec.Command("sh", "-c", cmdString)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	return err
}
#############################################################################################



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

func serve(conn *net.TCPConn) {
	reader := bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr := fs.GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				conn.Close()
				break
			}
		}

		response := fs.ProcessMsg(msg)
		if !reply(conn, response) {
			conn.Close()
			break
		}
	}
}

func startServer() {
	var clientId int
	clientId = 0
	host = "127.0.0.1"
	port = 6000 + id
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
		go serve(tcp_conn)
	}
}


func initializeRaft() {

	var configuration Config // // this configuration is in Config structure format
	var err error
	var configFile *os.File //file discriptor
	var filename string
	var decoder *json.Decoder
	//var config Config   

	filename = "config" + strconv.Itoa(id) + ".json"
	configFile, err = os.Open(filename)
	if err != nil {
		fmt.Println("server.go_initializeRaft_ERR_Opening_json")
	}
	defer configFile.Close()

	decoder = json.NewDecoder(configFile)
	err = decoder.Decode(&configuration)

	if err != nil {
		fmt.Println("server.go_initializeRaft_ERR_Docoding")
	}

	configuration.Id = id // selfid
	configuration.LogDir = strconv.Itoa(configuration.Id)

	raftnode = New(configuration)

}

func serverMain(serverId int) {
	id = serverId
	initializeRaft()
	go startServer()
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

	serverId = strconv.Atoi(os.Args[1])
	serverMain(serverId)
}