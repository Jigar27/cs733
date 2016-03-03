package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func Reading(t *testing.T, conn net.Conn) string {
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	buf := scanner.Text()

	if scanner.Err() != nil {
		t.Error("Cannot read from TCP socket..")
	}
	return buf
}

func Writing(t *testing.T, conn net.Conn, buf string) {
	_, err := conn.Write([]byte(buf + "\r\n"))
	if err != nil {
		t.Error("Cannot write to TCP socket..")
	}
}

func check(t *testing.T, result string, expected string) {
	if result != expected {
		t.Error(fmt.Sprintf("'%s' was expected but '%s' was received\n", expected, result))
	}
}

// Simple serial check of getting and setting
func TestTCPSimple(t *testing.T) {
	go serverMain()

        time.Sleep(1 * time.Second) // one second is enough time for the server to start
	name := "hi.txt"
	contents := "bye"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:9001")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	//fmt.Println(arr[0])
	expect(t, arr[0], "OK")

	version, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())

	checkExpiryHandler(t,conn)

	TestConcurrency(t)
}


	

func TestConcurrency(t *testing.T) {
	//we will create 10 clients 
	var n_client = 10
	var filename = "abc.txt"
	var n int64
	//var nReply int64
	rpl := make(chan int)

	for i:=0; i<n_client; i++ {
		content := fmt.Sprintf("client%d",i)
		go client(t, filename, content, ack)
		t.Log("client "+content+" has started")
	}		

	for j:=o; j<n_client; j++ {
		n=n+<-ack
	}

	if n!=1 {
		t.Error("Failed : TestConcurrency")	
	}
}

func client(t *testing.T, filename string, content string, ack chan int){
	//each client will write the same file and get the version 
	//at the end when each client finishes it's write last version will be one of these 10 client's version
	//then each client will perform cas 
	//only one client will succeed.
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		fmt.Println(err)
		t.Error("Cannot initialize TCP connection")
	}

	Writing(t,conn,"write "+filename+" 7 50")
	Writing(t,conn,content)

	r := Reading(t, conn)
	reply := strings.Split(r, " ")

	check(t,reply[0],"OK")


	Writing(t, conn, "cas "+filename+" "+reply[1]+" 50")
	Writing(t, conn, "content")

	// time.Sleep(time.Second)
	r = Reading(t, conn)
	reply = strings.Split(r, " ")
	if reply[0] == "OK" {
		ack <- 1
	} else {
		ack <- 0
	}
}
func checkExpiryHandler(t *testing.T,conn net.Conn) {

	
	t.Log("Testing expiry handler..")

	

	_, err1 := conn.Write([]byte("write aaa.txt 2 50" + "\r\n"))
	if err1 != nil {
		t.Error("Cannot write to TCP socket..")
	}


	_,err2 := conn.Write([]byte("ab"+"\r\n"))
	if err2 != nil {
		t.Error("Cannot write to TCP socket..")
	}

	
	//read from socket
	scanner1 := bufio.NewScanner(conn)
	scanner1.Scan()
	reply := scanner1.Text()
	if scanner1.Err() != nil {
		t.Error("Cannot read from TCP socket..")
	}

	
	response := strings.Split(reply, " ")

	

	if response[0] != "OK" {
		t.Error(fmt.Sprintf("Expected 'OK' but got '%s'\n", response[0]))
	}



	//Wait for 2 second
	time.Sleep(50 * time.Second)





	
	_,err3 := conn.Write([]byte("read aaa.text"+"\r\n"))
	if err3 != nil {
		t.Error("Cannot write to TCP socket..")
	}

	scanner2 := bufio.NewScanner(conn)
	scanner2.Scan()
	reply2 := scanner2.Text()
	if scanner2.Err() != nil {
		t.Error("Cannot read from TCP socket..")
	}

	response2 := strings.Split(reply2, " ")
	if response2[0] != "ERR_FILE_NOT_FOUND" {
		t.Error(fmt.Sprintf("Expected 'ERR_FILE_NOT_FOUND' but got '%s'\n", response2[0]))
	}
	
}




