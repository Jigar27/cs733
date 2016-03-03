package main  

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"math/rand"
	"time"
)


const PORT = ":9000"
const DEBUG = false

// file and metadata
type value struct {
	content []byte
	numBytes int64 
	version int64 
	expTime int64
}
//type files struct {
//	content []byte
//	numBytes int64
//	version int64
//	expTime int64
//}

type requestStruct struct {
	clientConn net.Conn
	command []string
	data string
	ack chan bool
}

var requestQ chan requestStruct

var m map[string]value

func debug(msg string) {
	if DEBUG {
		fmt.Println(msg)
	}
}

func closeConn(c net.Listener) {
	debug("Closing server..")
	c.Close()
}


func WriteTCP(clientConn net.Conn, data string) {
	//Write to TCP connection
	_, err := clientConn.Write([]byte(data))
	if err != nil {
		debug("Write Error:" + err.Error())
	}
}

func getLength(command []string) int64 {
	debug( "entering into getLength........")
	//fmt.Println(command[0])
	var l_string string = "0"
	fmt.Println(command[0])
	//debug("printing command[0] inside getLength() function.......")
	switch command[0]{
	case "write":
		debug("entering write case in getLength()")
		//numBytes,err := strconv.ParseInt(command[2],10,64)
		if len(command) > 2 {
			l_string = command[2]
		}
		

		//return numBytes
	case "cas":
		if len(command) > 3 {
			l_string = command[3]
		}
		//return numBytes
	}

	numBytes,err := strconv.ParseInt(l_string,10,64)
	if err != nil {
		debug("Invalid number of bytes specified.")
		return 0
	}
	if numBytes < 0 {
		debug("Number of Bytes must be postive integer")
		return 0
	}
	fmt.Println(numBytes)
	return numBytes
}


func requesthandler() {
	// process the request from requesQ
	for{
		request := <-requestQ
		debug("Request received : " + request.command[0])

		switch request.command[0] {

		case "write":
			//check whether command has appropriate number or arguments or not 
			if len(request.command) > 4 || len(request.command) < 3 {
				WriteTCP(request.clientConn,"ERR_CMD_ERR\r\n")
				continue
			}


			//check whether expiry time has been specified in the command or not.
			//If not, then set expTime to 0
			expTime, err1 := strconv.ParseInt(request.command[3],10,64)
			if len(request.command) > 3 {
				if err1 != nil {
					debug("Invalid expiry time specified.")
					WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
					continue
				}

				if expTime < 0 {
					debug("Expiry time has negative value")
					WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
					continue
				}

			} else {
				expTime = int64(0)
			}


			filename := request.command[1]
			

			


			//check whether number of bytes are proper or not 
			numBytes, err2 := strconv.ParseInt(request.command[2],10,64)
			if err2 != nil {
				debug("Invalid Number of Bytes soecified")
				WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
				return
			}
			if numBytes < 0 {
				debug("numBytes must be postive")
				WriteTCP(request.clientConn,"ERR_CMD_ERR\r\n")
				return
			}


			//check whether file already exists or not 
			_, ok := m[filename]
			var version int64
			if ok == true {
				debug("Filename already exists!")
				data := strings.TrimRight(request.data,"\r\n")
				v := m[filename]
				version = v.version
				version++
				m[filename]=value{[]byte(data), numBytes, version, expTime}
			} else {	

				data := strings.TrimRight(request.data,"\r\n")
				version = int64(rand.Intn(100000))
				m[filename]=value{[]byte(data), numBytes, version, expTime}
			}
			

			//a function after expiration needs to be sent to handler which will delete the expired file
			sendExpiry := func() { 
				ack := make(chan bool)
				requestQ <- requestStruct{nil,[]string{"expire",filename, fmt.Sprintf("%d",version)}, "", ack}
				<-ack
			}

			//set expiry timer
			if expTime > 0 {
				time.AfterFunc(time.Duration(expTime)*time.Second, sendExpiry)
			}


			WriteTCP(request.clientConn, fmt.Sprintf("OK %d \r\n", version))				
			request.ack <- true



		case "read":
			//check whether the command has appropriate number of arguments or not
			fmt.Println(len(request.command))
			if len(request.command) != 2 {
				WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
				request.ack<-true
				return
			}



			filename := request.command[1]
			v,ok := m[filename]
			if ok == false {
				WriteTCP(request.clientConn, "ERR_FILE_NOT_FOUND\r\n")
				request.ack<-true
				return
			}

			// if filename exists then send the contents of the file

			WriteTCP(request.clientConn, "CONTENTS "+fmt.Sprintf("%d %d %d",v.version,v.numBytes,v.expTime)+"\r\n")
			WriteTCP(request.clientConn, string(v.content)+"\r\n")

			request.ack<-true


		case "delete":

			//check whether the command has appropriate number of arguments or not
			
			if len(request.command) != 2 {
				WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
				request.ack<-true
				continue
			}

			filename := request.command[1]
			//check if FILE exists or not
			_,ok := m[filename]
			if ok == false {
				WriteTCP(request.clientConn, "ERR_FILE_NOT_FOUND\r\n")
				request.ack<-true
				continue
			}

			delete(m,filename)
			
			WriteTCP(request.clientConn, "OK\r\n")
			request.ack<-true

		case "cas":

			//check whether command has appropriate number or arguments or not 
			if len(request.command) > 5 || len(request.command) < 4 {

				WriteTCP(request.clientConn,"ERR_CMD_ERR\r\n")
				request.ack<-true
				return
			}


			filename := request.command[1]
			v,ok := m[filename]
			if ok == false {
				WriteTCP(request.clientConn, "ERR_FILE_NOT_FOUND\r\n")
				request.ack<-true
				return
			}

			casVersion,err1 := strconv.ParseInt(request.command[2],10,64)
			if err1 != nil {
				debug("Invalid Version specified")
				WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
				request.ack<-true
				return
			}

			

			if casVersion != v.version {
				WriteTCP(request.clientConn, fmt.Sprintf("ERR_VERSION %d \r\n",v.version))
				request.ack<-true
				return
			} 


			casVersion++


			expTime, err2 := strconv.ParseInt(request.command[4],10,64)
			if err2 != nil {
				debug("Invalid expiry time specified.")
				WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
				request.ack<-true
				return
			}

			if expTime < 0 {
				debug("Expiry time has negative value")
				WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
				request.ack<-true
				return
			}


			numBytes, err3 := strconv.ParseInt(request.command[3],10,64)
			if err3 != nil {
				debug("Invalid Number of Bytes soecified")
				WriteTCP(request.clientConn, "ERR_CMD_ERR\r\n")
				request.ack<-true
				return
			}
			if numBytes < 0 {
				debug("numBytes must be postive")
				WriteTCP(request.clientConn,"ERR_CMD_ERR\r\n")
				request.ack<-true
				return
			}

			data := strings.TrimRight(request.data,"\r\n")
			
			m[filename]=value{[]byte(data), numBytes, casVersion, expTime}

			WriteTCP(request.clientConn, fmt.Sprintf("OK %d \r\n", casVersion))
			request.ack<-true
		case "expire":
			filename := request.command[1]
			version,err := strconv.ParseInt(request.command[2], 10, 64)
			if err != nil {
				debug("Invalid version")
				return
			}


			v,ok := m[filename]
			if ok == false {
				debug("File doesnot exists or already deleted")
				return
			}

			if v.version == version {
				delete(m,filename)
				debug("expired "+filename+" got deleted")
			}
		}		
	}
	
}



func handleClient(clientConn net.Conn) {

	defer clientConn.Close()
	scanner := bufio.NewScanner(clientConn)

	for{
		//Split method of scanner will return one line (untill \r\nencounters)
		//the behaviour is similar to the grep command, sed command.
		scanner.Split(bufio.ScanLines)
		scanner.Scan()

		buf := scanner.Text()
		//debug("printing buffer........")
		fmt.Println(buf)
		//now the buffer contains the first line of what client has sent to server.
		//a proper request from client contains (read/write/delete/cas) command and parameters ending with \r\n

		if scanner.Err() != nil {
			debug("Command Read Error")
			WriteTCP(clientConn, "ERR_INTERNAL\r\n")
			return
		}

		//following Split method removes trailing and heading \r\n and breaks the streams into words seperated by " "
		command := strings.Split(strings.Trim(buf, "\r\n")," ")
		//command[0] holds a command name
		switch command[0]{
		case "read" :
			ack := make(chan bool)
			// requestStruct simply says that so & so client wants to issue so & so command and so & so data.
			requestQ <- requestStruct{clientConn, command, "", ack}
			<-ack 

		case "delete":
			ack := make(chan bool)
			requestQ <- requestStruct{clientConn, command, "",ack}
			<-ack
		
		case "write":
			debug("entering the write case in clienthandler ..........")
			length := getLength(command)
			fmt.Println(length)
			if length < 1 {
				WriteTCP(clientConn, "ERR_CMD_ERR\r\n")
				
				continue
			}
			//spliting the stream in bytes
			scanner.Split(bufio.ScanBytes)

			dataBytes := make([]byte, length)
			for i := int64(0); i < length ; i++ {
				debug("ahiya pahoche 6?")
				scanner.Scan() // returns the next token
				
				if scanner.Err() != nil {
					debug("Data Read Error")
					WriteTCP(clientConn, "ERR_INTERNAL\r\n")
					return
				}

				dataBytes[i] = scanner.Bytes()[0]
			}

			data := string(dataBytes)
			fmt.Println(data)
			ack := make(chan bool)
			debug("putting write request onto requestQ")
			requestQ <- requestStruct{clientConn, command, data, ack}	
			<- ack	

		case "cas":
			length := getLength(command)
			scanner.Split(bufio.ScanBytes)

			dataBytes := make([]byte,length)
			for i := int64(0); i < length ; i++ {
				scanner.Scan()

				if scanner.Err() != nil {
					debug("Data Read Error")
					WriteTCP(clientConn, "ERR_INTERNAL\r\n")
					return
				}

				dataBytes[i] = scanner.Bytes()[0]

			}

			data := string(dataBytes)
			ack := make(chan bool)
			requestQ <- requestStruct{clientConn, command, data, ack}
			<-ack

		
			
		}
	}

}




func serverMain() {

	debug("starting server.....")
	//for {
		conn, err := net.Listen("tcp", PORT)
		if err != nil {
			debug("Error listening to port: "+err.Error())
			//continue
		}
		defer closeConn(conn)

		//Initialize filestore
		m=make(map[string]value)

		//Initialize requestQ 
		//requestQ is a channel in which msg (structured as requestStruct) are being passed, which is typically a request made by defferent clients.
		requestQ = make(chan requestStruct,100)

		//requesthandler is a goroutine that handles the all requests made by different clients.
		// it uses a requestQ channel.
		go requesthandler()

		debug("server started.......")

		for {
			//wait for connections from clients
			client,err := conn.Accept()

			if err != nil {
				debug("Error accepting the connection: "+err.Error())
				continue
			}

			//Handle the client.........................................................................................................

			go handleClient(client)
		}

	//}
}



func main() {
	serverMain()
}