/*

A trivial application to illustrate how the fdlibx library can be used
in assignment 1 for UBC CS 416 2018W1.

Usage:
go run client.go
*/

package main

// Expects fdlibx.go to be in the ./fdlibx/ dir, relative to
// this client.go file
import (
	"../fdlib"
	"math/rand"
)

import "fmt"
import "os"
import "time"

func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8860"
	toMonitorIpPort := "127.0.0.1:8867" // TODO: change this to remote node
	var lostMsgThresh uint8 = 5

	// TODO: generate a new random epoch nonce on each run
	rand.Seed(time.Now().Unix())
	var epochNonce uint64 = rand.Uint64()
	var chCapacity uint8 = 5

	// Initialize fdlibx. Note the use of multiple assignment:
	// https://gobyexample.com/multiple-return-values
	fd, notifyCh, err := fdlib.Initialize(epochNonce, chCapacity)
	if checkError(err) != nil {
		return
	}

	// Stop monitoring and stop responding on exit.
	// Defers are really cool, check out: https://blog.golang.org/defer-panic-and-recover
	defer fd.StopMonitoring()
	defer fd.StopResponding()

	err = fd.StartResponding(localIpPort)
	if checkError(err) != nil {
		return
	}

	fmt.Println("Started responding to heartbeats.")

	// Add a monitor for a remote node.
	localIpPortMon := "127.0.0.1:9299"
	err = fd.AddMonitor(localIpPortMon, toMonitorIpPort, lostMsgThresh)

	//fd.StopResponding()
	if checkError(err) != nil {
		return
	}

	err = fd.AddMonitor("127.0.0.1:9300", "127.0.0.1:8868", lostMsgThresh)

	//fd.StopResponding()
	if checkError(err) != nil {
		return
	}

	err = fd.AddMonitor("127.0.0.1:9301", "127.0.0.1:8869", lostMsgThresh)

	//fd.StopResponding()
	if checkError(err) != nil {
		return
	}

	err = fd.AddMonitor("127.0.0.1:9302", "127.0.0.1:8870", lostMsgThresh)

	//fd.StopResponding()
	if checkError(err) != nil {
		return
	}

	err = fd.AddMonitor("127.0.0.1:9303", "127.0.0.1:8871", lostMsgThresh)

	//fd.StopResponding()
	if checkError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPort)

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	select {
	case notify := <-notifyCh:
		fmt.Println("Detected a failure of", notify)
		// Re-add the remote node for monitoring.
		err := fd.AddMonitor(localIpPortMon, toMonitorIpPort, lostMsgThresh)
		if checkError(err) != nil {
			return
		}
		fmt.Println("Started to monitor node: ", toMonitorIpPort)
	case <-time.After(time.Duration(int(lostMsgThresh)*200) * time.Second):
		// case <-time.After(time.Second):
		fmt.Println("No failures detected")
		//fd.StopMonitoring()
	}
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}