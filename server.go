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
	"time"
)

//A [conncetion, bool] key pair about the status of the client.
var isClientAlive = make(map[net.Conn]bool)

//a key pair value of connections and slaves
var slaves = make(map[net.Conn]*slave)

//a variable to save chunk info, like which node is having which data!
var chunkInfo = make(map[int]string)

//A connection client key value pair
var clients = make(map[net.Conn]*client)

//A structure to save all the information about
type slave struct {
	rank  int
	alive bool
	chunk []string
	busy  bool
	conn  net.Conn
}
type slaveinfo struct {
	conn  net.Conn
	chunk int
}
type client struct {
	slaves []*slaveinfo
	found  int
	alive  bool
}

func main() {
	getChunkInfo("./chunksinfo.txt")
	//	fmt.Println(getRequiredNodes())
	slavePort, clientPort := assignPort()
	slaveChannel := make(chan slave)

	aliveChannel := make(chan net.Conn)
	removeChannel := make(chan net.Conn)
	//User feedback
	fmt.Println("Slaves can connect at : " + toStr(slavePort))
	fmt.Println("Client can connect at : " + toStr(clientPort))
	//handle communication in a different thread
	go slaveCommunicationHandler(slaveChannel, aliveChannel, removeChannel)
	removeCl := make(chan net.Conn)
	//remove client in a different thread
	go removeClient(removeCl)
	//slave conncetion handler
	go slavesConnectionHandler(slavePort, slaveChannel, aliveChannel)
	//listen on the specified port for the slave
	listen, err := net.Listen("tcp", "127.0.0.1:"+toStr(clientPort))
	//error handling
	if err != nil {
		log.Fatal(err)
	}
	//as long as many connections are requested the server will provide them all
	for {
		connection, err := listen.Accept()
		//unless an error occurs
		if err != nil {
			log.Fatal(err)
		}
		//and it'll always handle connectin in a different thread
		go clientHandler(connection, removeCl)

	}
}

//This method is used to delete the client that is no longer engaged in any activity
func removeClient(removeChannel chan net.Conn) {
	//for as many clients
	for {
		//select all those which are in removeClient channel to remove them
		select {
		case connection := <-removeChannel:
			//if there is still a connection with the client
			if _, okay := clients[connection]; okay {
				//ask all the slaves to stop
				for _, slaveInfoV := range clients[connection].slaves {
					slaves[slaveInfoV.conn].busy = false
					//write a message to all the slave in the list to stop
					io.WriteString(slaveInfoV.conn, "stop"+"\n")
				}
				//and delete the client's connection
				delete(isClientAlive, connection)
			}
		}
	}
}

//A method to handle the connection with the client
func clientHandler(connection net.Conn, removeChannel chan net.Conn) {
	//new reader for the particular connection
	reader := bufio.NewReader(connection)
	//reading data from the buffer
	data, err := reader.ReadString('\n')
	//error handling
	if err != nil {
		return
	}
	//putting the trimmed data to password
	password := strings.Trim(data, "\n")
	//extacting 1st digit of the password
	password = password[0:]
	//User feedback
	fmt.Println("Client connected. ", "\nPassword to search is: ", password)
	//make the connection to the client alive in record
	isClientAlive[connection] = true
	//schedule search in a separate thread
	go searchScheduler(password, connection)
	//After it finishes searching client must left and that situation is handeled below
	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			isClientAlive[connection] = false
			removeChannel <- connection
			fmt.Println("Client has left after the search request!")
			return
		}
	}

}

//A method basically a schedule which will schedule a search for a password on a given connection
func schedule(duration time.Duration, password string, clientConnection net.Conn) {
	//getting required chunks in which the password lies.
	requiredChunks := getRequiredChunks(password)
	//enable this line to see which chunk contains the password for the overview
	//fmt.Println(requiredChunks)
	slaveInformaton := []*slaveinfo{}
	//heartbeat from the client, if we dont hear from the client for a duration, we consider it dead.
	for _ = range time.Tick(duration) {
		if isClientAlive[clientConnection] == false {
			//user feedback
			fmt.Println("client disconnected")
			return
		}
		//otherwise try to schedule on selected slaves who have the chuncks
		fmt.Println("Trying to Schedule search on the slave(s), please wait!")
		//if the required chunks appears to be more than one, then , schedule on every slave
		if len(requiredChunks) > 0 {
			for i, rCounter, rlength := 0, 0, len(requiredChunks); i < rlength; i++ {
				j := i - rCounter
				//check if the clients are still in connection
				for slaveconnection, slaveIn := range slaves {
					check := false
					//and not busy
					if slaveIn.busy == false {
						//then make check = true
						for _, f := range slaveIn.chunk {
							if toInt(f) == requiredChunks[j] {
								//right here
								check = true
								break
							}
						} //and if all goes fine , start scheduling on the slaves(Selected ones)
						if check {
							fmt.Println("Scheduling on ", slaveIn.rank)
							//make them busy
							slaveIn.busy = true
							//make their connection occupied
							slaveIn.conn = clientConnection

							info := &slaveinfo{conn: slaveconnection, chunk: requiredChunks[j]}
							slaveInformaton = append(slaveInformaton, info)

							requiredChunks = append(requiredChunks[:j], requiredChunks[j+1:]...)
							rCounter++

							break
						}
					}
				}
			}
			//if the slaves that contain chunks are not connected, wait for them to connect and retry!
			if len(requiredChunks) > 0 {
				fmt.Println(requiredChunks, " are not scheduled yet, maybe the slaves aren't all connected!")
			}
		} else {
			break
		}
	}
	if isClientAlive[clientConnection] == false {
		fmt.Println("clientleft the connection!")
		return
	}
	//User feedback
	fmt.Println("Waiting for the results... ")
	//
	clients[clientConnection] = &client{slaves: slaveInformaton, found: 0}
	fmt.Println(clients[clientConnection].slaves)
	//for every slave that has chunck of data, prepare a piece of info containing chunck number and
	//and password to search in order to start
	for _, slaveInfoV := range clients[clientConnection].slaves {
		//this will make something like "search[chunknumber][password]"
		tosend := "search" + toStr(slaveInfoV.chunk) + password + "\n"
		//get it busy with
		slaves[slaveInfoV.conn].busy = true
		slaves[slaveInfoV.conn].conn = clientConnection
		//User feedback
		fmt.Println("Scheduling on ", slaves[slaveInfoV.conn].rank)
		//write prepared data to slave
		io.WriteString(slaveInfoV.conn, tosend)
	}
}
func searchScheduler(password string, conn net.Conn) {
	//for every two second try to schedule if not succeeded
	schedule(2000*time.Millisecond, password, conn)

}

//heartbeat to test if a connection to slave or client is alive or not
func heartbeat(duration time.Duration, aliveChannel chan net.Conn, conn net.Conn) {
	//after every duration time make sure that the connections are still alive
	for _ = range time.Tick(duration) {
		if slaves[conn].alive == false {
			aliveChannel <- conn
			break

		} else if slaves[conn].alive == true {
			slaves[conn].alive = false

		}

	}
}

func slaveHandler(conn net.Conn, slaveChannel chan slave, aliveChannel chan net.Conn) {
	//heartbeat every 4 seconds
	go heartbeat(4000*time.Millisecond, aliveChannel, conn)
	//a new connection
	reader := bufio.NewReader(conn)

	for {
		//read data from the string came from slave
		data, err := reader.ReadString('\n')
		//error handling
		if err != nil {
			if _, okay := slaves[conn]; okay {
				slaves[conn].alive = false
			}
			return
		}
		//trim the data to the delimeter in order to avoid garbage
		message := strings.Trim(data, "\n")
		//if data has message starting from rank, it means its a new connection information
		if strings.HasPrefix(message, "rank") {
			//slice up data to extract usefull information
			rank := toInt(message[4:5])
			//user feedback
			fmt.Println("Rank : ", rank, "\nStatus : Connected")
			//make all the attributes enabled
			slaves[conn] = &slave{rank: rank, alive: true, busy: false, conn: conn}
			//and if data has message starting from alive it means a heartbeat
		} else if strings.HasPrefix(message, "alive") {
			//just making sure that the connection is alive and stuff
			slaves[conn].alive = true //heartbeat
			//and if data has a message starting from chunk, its giving information about the chunks
		} else if strings.HasPrefix(message, "chunk") {
			chunks := strings.Split(message, ":")[1:] //coz 0th index is message type
			slaves[conn].chunk = chunks
			//fmt.Println(slaves[conn].chunk)

		} else if strings.HasPrefix(message, "found") {
			fmt.Println("Password Found BY slave ", slaves[conn].rank)
			otherSlaves := clients[slaves[conn].conn].slaves
			//send a stop signal
			slaves[conn].busy = false

			for _, v := range otherSlaves {
				if conn != v.conn {
					slaves[v.conn].busy = false
					io.WriteString(v.conn, "abort"+"\n") //sending an abort message
				}
			}
			io.WriteString(slaves[conn].conn, "found"+"\n")
			//if not found
		} else if strings.HasPrefix(message, "no") {
			slaves[conn].busy = false
			fmt.Println(slaves[conn].rank, " : Password not found ")
			clients[slaves[conn].conn].found++

			ttl := len(clients[slaves[conn].conn].slaves)

			if ttl == clients[slaves[conn].conn].found {
				io.WriteString(slaves[conn].conn, "notfound"+"\n")

			}

		}

	}

}

//this method handles the connection request for slaves
func slavesConnectionHandler(slavePort int, slaveChannel chan slave, aliveChannel chan net.Conn) {
	listen, err := net.Listen("tcp", "127.0.0.1:"+toStr(slavePort))
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go slaveHandler(conn, slaveChannel, aliveChannel)

	}

}

//
func slaveCommunicationHandler(slaveChannel chan slave, aliveChannel chan net.Conn, removeChannel chan net.Conn) {
	for {
		select {

		case alive := <-aliveChannel:
			if slaves[alive].alive == true {
				fmt.Println(toStr(slaves[alive].rank) + "is alive")

			} else {
				fmt.Println(toStr(slaves[alive].rank) + " has disconnected ☠")
				if slaves[alive].busy == true {
					clcon := slaves[alive].conn
					clients[clcon].found++
					ind := 0
					for i, v := range clients[clcon].slaves {
						if v.conn == alive {
							ind = i
							break
						}
					}
					clients[clcon].slaves = append(clients[clcon].slaves[:ind], clients[clcon].slaves[ind+1:]...)

					if clients[clcon].found >= len(clients[clcon].slaves) {
						io.WriteString(clcon, "notfound"+"\n")
					}
				}

				go func() { //to prevent deadlock. coz channels are blocking
					removeChannel <- alive
				}()

			}
		case slave := <-removeChannel:

			delete(slaves, slave)
			printconnected()

		}
	}
}
func printconnected() {
	for i, v := range slaves {
		fmt.Println(i, " Rank  : ", toStr(v.rank), ", status : connected ")
	}

}

//****************************************************************************************************
//helper functions
func getChunkInfo(path string) {
	inFile, _ := os.Open(path)
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {

		chunks := strings.Split(scanner.Text(), "=")

		v := toInt(string(chunks[0]))
		chunkInfo[v] = chunks[1]

	}
	//	fmt.Println(chunkInfo)
}
func getRanks() []int {
	var req []int
	for _, v := range slaves {
		req = append(req, v.rank)
	}
	return req
}
func getRequiredChunks(toSearch string) []int {
	password := strings.ToLower(string(toSearch[0]))
	var req []int

	for i, v := range chunkInfo {
		sum := cmp(string(v[0]), password) + cmp(string(v[1]), password)
		if sum >= -1 && sum <= 1 {
			req = append(req, i)

		}
	}
	return req
}

//conversion functions.
func toInt(str string) int {
	c, _ := strconv.Atoi(string(str))
	return c
}
func toStr(integer int) string {
	return strconv.Itoa(integer)
}
func cmp(a string, b string) int {
	return strings.Compare(a, b)
}
func assignPort() (int, int) {
	slavePort := 9000
	clientPort := 7000
	if !(len(os.Args) < 3) {
		clientPort = toInt(os.Args[1])
		slavePort = toInt(os.Args[2])

	}
	return slavePort, clientPort
}
