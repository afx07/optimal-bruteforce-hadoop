package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

//methods below are helper methods separated in order to avoid any complications
func convertPortToInteger(str string) int { //A helper function to convert a given string to integer
	integer, _ := strconv.Atoi(string(str)) //conversion builtin method
	return integer
}

//A helper method to convert a given integerValue to string
func convertPortToString(integerValue int) string {
	return strconv.Itoa(integerValue) //conversion method
}

//This method extracts port number and paswword to search from command line arguments
func assignPortNPass() (int, string) {

	//To store the port number of the incoming client
	var portNo int
	//This variable stores the password to be searched
	var passToSearch string

	if !(len(os.Args) < 3) { //Run this if there are enough arguments in the command
		//portNo below takes the port number from first argument
		portNo = convertPortToInteger(os.Args[1])
		//passToSearch takes password to search from 2nd argument
		passToSearch = os.Args[2]

	} else { //else display the error and instruction to rewrite
		log.Fatal("Please specify port Number and Password to search alongwith the command.")
	}
	//finally return the recieved arguments respectively.
	return portNo, passToSearch
}

//The Main Function
func main() {
	//gets assigned port number and password to search from command line arguments
	portNo, passToSearch := assignPortNPass()
	//User feedback
	fmt.Println("Connecting server at port number : " + convertPortToString(portNo))
	//User feedback
	fmt.Println("Password to search is: " + passToSearch)
	//establishing connection with the server
	connInstance, err := net.Dial("tcp", "127.0.0.1:"+convertPortToString(portNo))
	//error/Exception handling
	if err != nil {
		log.Fatal(err)
	}
	//Send password to search to server using connection string
	io.WriteString(connInstance, passToSearch+"\n")
	//open a buffer reader for the particular connection inorder to return "found" or "not found" signal
	reader := bufio.NewReader(connInstance)
	//read the returned signal until the delimeter of a new line
	signalFromServer, err := reader.ReadString('\n')
	//error handling and exception handling
	if err != nil {
		log.Fatal("Error in finding password. Server, probably, Left!")
		return
	}
	//if server returns found message, print the found message
	if strings.HasPrefix(signalFromServer, "found") {
		fmt.Print("Password" + passToSearch + " was found!\n")
	} else { //print "not found message"
		fmt.Print("Password " + passToSearch + " was not found!\n")
	}
}
