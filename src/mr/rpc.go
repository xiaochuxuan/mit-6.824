package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Type     int
// 	filename string // the filename of map task
// }

// the current state of the worker or the coordinator
const (
	State_Free   int = 0
	State_Map    int = 1
	State_Reduce int = 2
	State_Exit   int = 3
)

type Req struct {
	WorkerID int64 // the identify of the worker which sends request
}

// the task of map or reduce
type Task struct {
	Type     int       // task type: map or reduce
	ID       int       // task ID
	NReduce  int       // the number of reduce tasks
	FileName string    // the file name: input file
	FileList *[]string // intermediate file list
}

// when a worker finished the work, it will send a signal to it
type Task_Finish struct {
	TaskType int
	TaskID   int
	WorkerID int64
}

// return
type Success struct {
	Success bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
