package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

//Abort variable bool set to false before any processing started
var abort = false

//conversion functions.
func convertToInteger(str string) int {
	converted, _ := strconv.Atoi(string(str))
	return converted
}

func convertToString(integer int) string {
	return strconv.Itoa(integer)
}

//A helper method to print endline, necessary in transferring data to the server
func endl() string {
	return "\n"
}

//heartbeat method to acknowledge wheather a connection is alive or not before the actual message is recieved.
func heartBeatHandler(duration time.Duration, KeepAlive chan bool) {
	//a loop to make KeepAlive true after every duration is passed
	for _ = range time.Tick(duration) {
		KeepAlive <- true
	}
}

//A helper method to extract and assign file from the filessystem in order slaves connect to the server
func getFilesList(rankNumber int) string {
	//building path to the FolderOnNode based on ranks
	FolderOnNode, error := ioutil.ReadDir("../node" + convertToString(rankNumber))
	//Error and Exceptin handling
	if error != nil {
		log.Fatal(error)
	}
	//user feedback
	splitInfo := "chunk"
	//for all the files present in the node's directory show the chunks to the user
	for _, file := range FolderOnNode {
		//get name of the file
		fileName := file.Name()
		//if file is starting from the prefix chunk, bingo, its our file :P
		if strings.HasPrefix(fileName, "chunk") {
			//extract chunk number from the file name
			splitInfo += ":" + fileName[5:6]
		}

	}
	//returning Split Info or chunks
	return splitInfo
}

//Main method to find the password in the node's files on which the searching was scheduled
func findPassword(pathToFile string, password string, feedbackChannel chan string) {
	//Opening the file
	inputFile, _ := os.Open(pathToFile)
	//to see if the password was Found
	var isFound = false
	//to close the file successfully after the work is done
	defer inputFile.Close()
	//to read from the selected files
	reader := bufio.NewScanner(inputFile)
	//
	reader.Split(bufio.ScanLines)
	//while the reader has something to read in the file do...
	fmt.Println("Finding the Password ...")
	for reader.Scan() {
		line := reader.Text() //get a line from the file
		if abort == false {   //while abort is false
			//fmt.Println(line)
			if strings.Compare(string(line), password) == 0 {
				//fill the feedback cahnnel with found attribute/string
				feedbackChannel <- "found"
				//set the value to true and break
				println("Password found!")
				isFound = true
				break
			}
		} else {
			abort = false
			return
		}

	}
	if isFound == false {
		fmt.Println("Not found, Password doesn't exist")
		feedbackChannel <- "no"
		return
	}
	return
}

//The function handles communication between the server and slave.
func communicationHandler(connection net.Conn, KeepAlive chan bool, feedbackChannel chan string) {
	//for untill the slave is alive
	for {
		select {
		//send server "found" or "notfound" message
		case msg := <-feedbackChannel:
			io.WriteString(connection, msg+endl())
		//or send server the info that slave is alive and ready!
		case _ = <-KeepAlive:
			io.WriteString(connection, "alive"+endl())
		}
	}
}

//Method to extract port and rank number from command line arguments
func argumentHandler() (int, int) {
	var rankNumber int
	var portNumber int
	if len(os.Args) == 3 {
		portNumber = convertToInteger(os.Args[1]) //first argument is port number
		rankNumber = convertToInteger(os.Args[2]) //second argument is rank number

	} else if len(os.Args) < 3 { //Error if user inputs less than 3 or no arguments
		log.Fatal("CommandLine Arguments needed! Rank and serverPort")
	} else if len(os.Args) > 3 {
		log.Fatal("Too many Arguments. Please enter atmost three arguments")
	}
	return portNumber, rankNumber
}

//The Main Function
func main() {
	//channels necessary to transfer the information
	KeepAlive := make(chan bool)
	//to know if the password was found or not
	feedbackChannel := make(chan string)
	//get the port number and rank from the command line arguments
	portNumber, rankNumber := argumentHandler()
	//user feedback
	fmt.Println("Connecting to server at port: " + convertToString(portNumber))
	fmt.Println("Rank: " + convertToString(rankNumber))
	//Dialing the connection to cope up with the server
	connection, error := net.Dial("tcp", "127.0.0.1:"+convertToString(portNumber))
	//error handling
	if error != nil {
		log.Fatal(error)
	}
	//
	io.WriteString(connection, "rank"+convertToString(rankNumber)+endl())
	//get the information where the files are placed
	splitInfo := getFilesList(rankNumber)
	//Send the Split Info to the server
	io.WriteString(connection, splitInfo+endl())
	//let the different thread run the heartbeat method to update keepAlive channel
	go heartBeatHandler(3000*time.Millisecond, KeepAlive)
	//handle communocation for each and every node that is connected to the server
	go communicationHandler(connection, KeepAlive, feedbackChannel)

	for {
		//read the connection string came from the server
		serverReader := bufio.NewReader(connection)
		//reading the data from the server uptill the delimeter
		dataFromServer, error := serverReader.ReadString('\n')
		//trimming the data to avoid any garbage
		data := strings.Trim(dataFromServer, "\n")
		//If the server has the search command, we would extract other usefull info like chunk
		if strings.HasPrefix(data, "search") {
			//[6:7] slice contains the chunk number in which to search
			indexNumber := data[6:7]
			//[7:] chunk have the password uptill next line
			password := data[7:]
			//concatinating to make a full file path
			fileName := "chunk" + indexNumber + ".txt"
			//making a full path
			fullPath := "../node" + indexNumber + "/" + fileName

			fmt.Println("password to search is: ", password)
			//starting to find password in a new thread
			go findPassword(fullPath, password, feedbackChannel)

		} else if strings.HasPrefix(data, "abort") { //if server issues an abort command
			//This could happen in two cases, which are, if the client's left or
			//an other slave has found the password.
			abort = true
			fmt.Println("Stopping Slave on server Command")
		}
		if error != nil {
			//Error Handling
			log.Fatal(error)
			return
		}
	}
}
