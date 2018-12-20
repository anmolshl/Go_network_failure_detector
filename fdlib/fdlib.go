/*

This package specifies the API to the failure detector library to be
used in assignment 1 of UBC CS 416 2018W1.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Initialize, but you
cannot change its API.

*/

package fdlib

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

//////////////////////////////////////////////////////
// Define the message types fdlib has to use to communicate to other
// fdlib instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fdlib instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

type MyFD struct {
	EpochNonce, currSeq uint64
	LiveHBs []SentHB
	initialized bool
	receivedHB bool
	startedResponding bool
	rttCache []rttCacheStruct
	hbeatCache map[uint64]HBeatMessage
	remoteSeqCache map[string]uint64
	timeStampCache []timeStampCacheStruct
	stopMonitoringChannelCache map[string]chan struct{}
	mux sync.Mutex
}

type SentHB struct {
	localAddr string
	remoteAddr string
	lostMessagesThresh uint8
}

type RTTRecord struct {
	RemoteIPPort string
	currRTT time.Duration
}

type rttCacheStruct struct {
	mux sync.Mutex
	remAddr string
	rtt time.Duration
}

type timeStampCacheStruct struct {
	mux sync.Mutex
	remAddr string
	timestamp time.Time
}

//////////////////////////////////////////////////////

// An FD interface represents an instance of the fd
// library. Interfaces are everywhere in Go:
// https://gobyexample.com/interfaces
type FD interface {
	// Tells the library to start responding to heartbeat messages on
	// a local UDP IP:port. Can return an error that is related to the
	// underlying UDP connection.
	StartResponding(LocalIpPort string) (err error)

	// Tells the library to stop responding to heartbeat
	// messages. Always succeeds.
	StopResponding()

	// Tells the library to start monitoring a particular UDP IP:port
	// with a specific lost messages threshold. Can return an error
	// that is related to the underlying UDP connection.
	AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error)

	// Tells the library to stop monitoring a particular remote UDP
	// IP:port. Always succeeds.
	RemoveMonitor(RemoteIpPort string)

	// Tells the library to stop monitoring all nodes.
	StopMonitoring()
}

const MULT_INIT_MSG = "Too many invocations!"
const EXST_MONI_MSG = "Monitoring already in progress!"
const NONC_MISM_MSG = "Nonce not matched!"
const SEQU_MISM_MSG = "Sequence mismatch!"
const BUFF_SIZE_VAL = 1024

var myFD MyFD
var notifyChannel chan FailureDetected

var stopRespondingChannel = make(chan bool)
var stopMonitoringChannel = make(chan bool)
var removeMonitorChannel = make(chan bool)

// The constructor for a new FD object instance. Note that notifyCh
// can only be received on by the client that receives it from
// initialize:
// https://www.golang-book.com/books/intro/10
func Initialize(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	if(myFD.initialized) {
		var fd MyFD
		return fd, nil, errors.New(MULT_INIT_MSG)
	}

	myFD = MyFD{}

	myFD.EpochNonce = EpochNonce
	myFD.initialized = true
	myFD.LiveHBs = []SentHB{}
	myFD.rttCache = []rttCacheStruct{}
	myFD.hbeatCache = make(map[uint64]HBeatMessage)
	myFD.timeStampCache = []timeStampCacheStruct{}
	myFD.remoteSeqCache = make(map[string]uint64)
	myFD.stopMonitoringChannelCache = make(map[string]chan struct{})
	myFD.currSeq = 0
	myFD.startedResponding = false

	notifyChannel = make(chan FailureDetected, ChCapacity)

	return myFD, notifyChannel, nil
}

func (fd MyFD) StartResponding(UDPIpPortLocal string) error {

	if fd.startedResponding {
		return errors.New("Already responding!")
	}

	localAddr, err:= net.ResolveUDPAddr("udp", UDPIpPortLocal)

	conn, err := net.ListenUDP("udp", localAddr)
	fmt.Println("Listening for HBs...")
	if err != nil {
		return err
	}
	quit := make(chan struct{})
	fd.startedResponding = true
	go fd.listenToHB(conn, quit)

	select {
	case <-stopRespondingChannel:
		fmt.Println("Stopped responding to all nodes!")
		conn.Close()
	default:

	}

	return nil

}

func (fd MyFD) listenToHB(conn *net.UDPConn, quit chan struct{}) {
	buf := make([]byte, BUFF_SIZE_VAL)
	length, err, remoteAddr := 0, error(nil), new(net.UDPAddr)
	for err == nil {
		length, remoteAddr, err = conn.ReadFromUDP(buf)
		if err != nil {
			fd.handleError(err, conn, remoteAddr.String(), "5")
			break
		}
		buffer := bytes.NewBuffer(buf[:length])
		decoder := gob.NewDecoder(buffer)
		var recHB HBeatMessage
		decoder.Decode(&recHB)
		fmt.Println("Received HB from " + remoteAddr.String())
		ackReply := AckMessage{recHB.EpochNonce, recHB.SeqNum}
		var bufferReply bytes.Buffer
		encoder := gob.NewEncoder(&bufferReply)
		encoder.Encode(ackReply)
		time.Sleep(time.Second*1)
		_, err = conn.WriteToUDP(bufferReply.Bytes(), remoteAddr)
		fmt.Println("Sent ack to " + remoteAddr.String())
		select {
		case <-stopRespondingChannel:
			fmt.Println("Stopped Responding!")
			conn.Close()
			return
		default:

		}
	}

	fmt.Println("listener failed - ", err)
	if err != nil {
		fd.handleError(err, conn, "", "6")
	}
	quit <- struct{}{}
	return
}

func (fd MyFD) StopResponding() {
	//TODO
	stopRespondingChannel<-true
	return
}

/*
* Check cache for similar AddMonitor arguments. Establish UDP between local and remote nodes, add to FD's list of
* current AddMonitor arguments that have been called, send heartbeats every RTT seconds.
*/
func (fd MyFD) AddMonitor(UDPIpPortLocal, UDPIpPortRemote string, lostMessagesThresh uint8) error {

	//TODO
	newHB := SentHB{UDPIpPortLocal, UDPIpPortRemote, lostMessagesThresh}

	chkRes, chkErr, hbRes := fd.checkCasesToMonitor(newHB)

	switch chkRes {

	case 1:
		return chkErr

	case 2:
		return chkErr

	case 3:
		hbRes.lostMessagesThresh = lostMessagesThresh
		break
	default:

	}

	var errorx error

	fd.mux.Lock()
	fd.LiveHBs = append(fd.LiveHBs, newHB)
	fd.stopMonitoringChannelCache[UDPIpPortRemote] = make(chan struct{})
	fd.mux.Unlock()

	go func() {
		conn, err := formUDPConn(UDPIpPortLocal, UDPIpPortRemote)
		fmt.Println("Listening for Acks...")
		if err != nil {
			fd.handleError(err, conn, UDPIpPortRemote, "7")
			return
		}

		quit := make(chan struct{})

		go fd.listenToAcks(conn, lostMessagesThresh, newHB, quit)
	}()

	return errorx
}

func (fd MyFD) listenToAcks(conn *net.UDPConn, lostMessagesThresh uint8, newHB SentHB, quit chan struct{}) {

	heartBeat := HBeatMessage{fd.EpochNonce, fd.currSeq}
	fd.mux.Lock()
	fd.rttCache = append(fd.rttCache, rttCacheStruct{sync.Mutex{}, newHB.remoteAddr, time.Second * 3})
	fd.hbeatCache[heartBeat.SeqNum] = heartBeat
	fd.remoteSeqCache[newHB.remoteAddr] = heartBeat.SeqNum
	fd.mux.Unlock()

	remoteAddr, err := net.ResolveUDPAddr("udp", newHB.remoteAddr)

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(heartBeat)
	lmt:=lostMessagesThresh

	rttx, _ := fd.getRtt(newHB.remoteAddr)

	for err == nil && lmt > 0 {
		if conn != nil {
			_, err = conn.WriteToUDP(buffer.Bytes(), remoteAddr)
		} else {
			fmt.Println("Nil")
			return
		}
		if err != nil {
			fd.handleError(err, conn, newHB.remoteAddr, "1")
			return
		}
		fmt.Println("Sent HB to " + newHB.remoteAddr)
		crrTime:=time.Now()
		fd.mux.Lock()
		if len(fd.timeStampCache) > 0 {
			tx, _ := fd.getTimestamp(newHB.remoteAddr)
			if tx == nil {
				fd.timeStampCache = append(fd.timeStampCache, timeStampCacheStruct{sync.Mutex{}, newHB.remoteAddr, crrTime})
			} else {
				*tx = timeStampCacheStruct{sync.Mutex{}, newHB.remoteAddr, crrTime}
			}
		}else {
			fd.timeStampCache = append(fd.timeStampCache, timeStampCacheStruct{sync.Mutex{}, newHB.remoteAddr, crrTime})
		}
		fd.mux.Unlock()
		go func() {
			buf := make([]byte, BUFF_SIZE_VAL)
			length:=0
			var addr net.Addr

			if conn != nil {
				lenx, address, errx := conn.ReadFromUDP(buf)
				err = errx
				length = lenx
				addr = address
			}else {
				fmt.Println("nil")
				select {
				case <-fd.stopMonitoringChannelCache[remoteAddr.String()]:
					return
				}
			}
			//fd.handleError(err, conn, newHB.remoteAddr, "2")
			buffer := bytes.NewBuffer(buf[:length])
			decoder := gob.NewDecoder(buffer)
			var recAck AckMessage
			decoder.Decode(&recAck)
			if recAck.HBEatEpochNonce != 0 {
				if recAck.HBEatEpochNonce != fd.EpochNonce {
					fmt.Println(NONC_MISM_MSG)
					fmt.Println(recAck.HBEatEpochNonce)
				} else if fd.currSeq < recAck.HBEatSeqNum || fd.remoteSeqCache[addr.String()] != recAck.HBEatSeqNum {
					fmt.Println(SEQU_MISM_MSG)
				}
				quit <- struct{}{}
			}
		} ()
		select {
		case <-quit:
			fmt.Println("Received ack from " + newHB.remoteAddr)
			lmt = lostMessagesThresh
			tmstmp, i := fd.getTimestamp(remoteAddr.String())
			fmt.Println(tmstmp.timestamp)
			newRTT := (rttx.rtt + time.Since(tmstmp.timestamp)) / 2
			fmt.Println(newRTT)
			fmt.Println(len(fd.rttCache))
			fd.mux.Lock()
			rttx.mux.Lock()
			if newRTT > time.Second {
				rttx.rtt = newRTT
			}
			rttx.mux.Unlock()
			if i > 0 {
				fd.timeStampCache = append(fd.timeStampCache[0:i], fd.timeStampCache[i+1:]...)
			}
			fd.mux.Unlock()
		case <-time.After(rttx.rtt):
			lmt--
		case <-fd.stopMonitoringChannelCache[newHB.remoteAddr]:
			fmt.Println("Removing monitor " + newHB.remoteAddr)
			conn.Close()
			conn = nil
			close(fd.stopMonitoringChannelCache[newHB.remoteAddr])
			fd.mux.Lock()
			delete(fd.stopMonitoringChannelCache, newHB.remoteAddr)
			fd.mux.Unlock()
			removeMonitorChannel<-true
			//removeMonitorChannel<-true
			return
		}

	}
	err = errors.New("Timeout")
	fmt.Println("listener for acks failed - ", err)
	if err != nil {
		fd.handleError(err, conn, newHB.remoteAddr, "3")
		return
	}

	return
}

/*
* Creates UDP {local, remote} pair
*/
func formUDPConn(local, remote string) (*net.UDPConn, error) {
	localAddr, err := net.ResolveUDPAddr("udp", local)
	conn, err := net.ListenUDP("udp", localAddr)
	return conn, err
}

/*
* Checks cases for different cases where a previous call to AddMonitor with similar arguments could have existed.
* a) Returns 1 when ALL arguments are the same. This means there is already an instance of AddMonitor running with
* 	 these arguments.
* b) Returns 2 when local addr:port OR remote addr:port AND lmt are the same as in a previous entry. This case exists
* 	 because only one port should be assigned for sending heartbeats to a remote node and receving packets from that
* 	 same remote node.
* c) Returns 3 when remote addr:port is the same as in a previous record AND lmt is different. In this case, the lmt
*    in the cache needs to be updated. The rest of AddMonitor is executed normally in this case.
* d) Returns 0 otherwise
*/
func (fd MyFD) checkCasesToMonitor(hb SentHB) (int, error, *SentHB) {
	result := 0

	var hbNil SentHB

	for i := range fd.LiveHBs {
		if fd.LiveHBs[i].lostMessagesThresh == hb.lostMessagesThresh && fd.LiveHBs[i].remoteAddr == hb.remoteAddr && fd.LiveHBs[i].localAddr == hb.localAddr {
			return 1, errors.New(EXST_MONI_MSG), &hbNil
		} else if fd.LiveHBs[i].lostMessagesThresh == hb.lostMessagesThresh && (fd.LiveHBs[i].remoteAddr == hb.remoteAddr || fd.LiveHBs[i].localAddr == hb.localAddr) {
			return 2, errors.New(EXST_MONI_MSG), &hbNil
		} else if fd.LiveHBs[i].remoteAddr == hb.remoteAddr && fd.LiveHBs[i].lostMessagesThresh != hb.lostMessagesThresh {
			return 3, nil, &hb
		}
	}

	return result, nil, &hbNil
}

func (fd MyFD) notifyOfFailure(remoteIPPort string) {

}

func (fd MyFD) StopMonitoring() {
	go func() {
		for i := range fd.stopMonitoringChannelCache {
			fd.mux.Lock()
			fd.stopMonitoringChannelCache[i] <- struct{}{}
			_, x := fd.getRtt(i)
			if x > 0 {
				fd.rttCache = append(fd.rttCache[:x], fd.rttCache[x+1:]...)
			}
			fd.mux.Unlock()
			<-removeMonitorChannel
			fmt.Println("Removed monitor " + i)
		}
		stopMonitoringChannel<-true
	}()
	select {
	case <-stopMonitoringChannel:
		fmt.Println("Stopped monitoring all nodes")
	}
	return
}

func (fd MyFD) RemoveMonitor(UDPIpPortRemote string) {
	fd.stopMonitoringChannelCache[UDPIpPortRemote] <- struct{}{}
	fd.mux.Lock()
	_, i := fd.getRtt(UDPIpPortRemote)
	fd.rttCache = append(fd.rttCache[:i], fd.rttCache[i+1:]...)
	fd.mux.Unlock()
	return
}

func (fd MyFD) handleError(err error, conn *net.UDPConn, remoteIPPort, index string) {
	if err!=nil {
		fmt.Println("Error occurred, notifying FD: " + index)
		fmt.Println(err)
		if conn != nil {
			conn.Close()
		}
		notifyChannel <- FailureDetected{remoteIPPort, time.Now()}
	}
	return
}

func (fd MyFD) getRtt(remAddr string) (*rttCacheStruct, int) {
	var result *rttCacheStruct
	result = nil
	ind:=0
	for i:=range fd.rttCache{
		if fd.rttCache[i].remAddr == remAddr {
			result = &fd.rttCache[i]
			ind = i
			break
		}
	}
	return result, ind
}

func (fd MyFD) getTimestamp(remAddr string) (*timeStampCacheStruct, int) {
	var result *timeStampCacheStruct
	result = nil
	ind:=0
	for i:=range fd.rttCache{
		if fd.rttCache[i].remAddr == remAddr {
			result = &fd.timeStampCache[i]
			ind = i
			break
		}
	}
	return result, ind
}