package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	stage int

	MapChan    chan *Task // the queue of map tasks
	ReduceChan chan *Task // the queue of Reduce tasks

	Nmap    int // the number of map tasks
	Nreduce int // the number of reduce tasks

	MapFinished    int // the number of finished map tasks
	ReduceFinished int // the number of finished reduce tasks
}

var c Coordinator // the coordinator

var mutex sync.Mutex // use lock to guarantee that the workers get tasks one by one

// Store the workers corresponding to the task, -1 indicates the job is finished or not assigned
var maptask_worker map[int]int64
var reducetask_worker map[int]int64

// Store the state of tasks: finish or not
var map_fini_list []bool
var reduce_fini_list []bool

// store the list of files corresponding to each reduce task
var reduce_list [][]string

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Handler(args *Req, reply *Task) error {

	mutex.Lock()         // lock
	defer mutex.Unlock() // unlock after exit the handler

	// fmt.Printf("c.stage: %d\n", c.stage)

	// Assign tasks according to stage
	switch c.stage {

	case State_Map:
		// take out the task from chan
		if len(c.MapChan) > 0 {
			*reply = *(<-c.MapChan)
			maptask_worker[reply.ID] = args.WorkerID

			log.Printf("the worker %v get the map task%v", args.WorkerID, reply.ID)

			// Open a new thread to monitor the worker
			go Monitor_map(args.WorkerID, reply)

		} else {
			*reply = c.NoTask()
		}

	case State_Reduce:
		if len(c.ReduceChan) > 0 {
			*reply = *(<-c.ReduceChan)
			reducetask_worker[reply.ID] = args.WorkerID

			log.Printf("the worker %v get the reduce task%v", args.WorkerID, reply.ID)

			// open a new thread to monitor the worker
			go Monitor_reduce(args.WorkerID, reply)

		} else {
			*reply = c.NoTask()
		}

	case State_Exit:
		*reply = c.ExitTask()

	default:
		*reply = c.NoTask()
	}

	return nil
}

// when a msg of finishment received, call it
func (c *Coordinator) CheckSuccess(args *Task_Finish, reply *Success) error {

	mutex.Lock()
	defer mutex.Unlock()

	if args.TaskType == State_Map && map_fini_list[args.TaskID] == false {
		map_fini_list[args.TaskID] = true

		c.MapFinished++

		// reset
		for k, v := range maptask_worker {
			if v == args.WorkerID {
				maptask_worker[k] = -1
			}
		}

		// All the map tasks have been finished
		if c.MapFinished == c.Nmap {
			// log.Printf("The Map tasks complete, start the Reduce tasks")

			c.stage = State_Reduce

			// call the Integrate function to fill the reduce_list
			Integrate()
			// send the reduce tasks
			c.ReduceTask()
		}

	} else if args.TaskType == State_Reduce && reduce_fini_list[args.TaskID] == false {
		reduce_fini_list[args.TaskID] = true

		c.ReduceFinished++

		// reset
		for k, v := range reducetask_worker {
			if v == args.WorkerID {
				reducetask_worker[k] = -1
			}
		}

		// All the reduce tasks have been finished
		if c.ReduceFinished == c.Nreduce {
			c.stage = State_Exit
		}
	}

	reply.Success = true

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	mutex.Lock()
	// Your code here.
	if c.stage == State_Exit {
		mutex.Unlock()
		time.Sleep(time.Duration(5) * time.Second)
		ret = true
		mutex.Lock()
	}

	mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c = Coordinator{
		stage:          State_Map,
		MapChan:        make(chan *Task, len(files)),
		ReduceChan:     make(chan *Task, nReduce),
		Nmap:           len(files), // the number of map tasks equals the number of input files
		Nreduce:        nReduce,
		MapFinished:    0,
		ReduceFinished: 0,
	}

	// fmt.Println(c)

	// Your code here.

	map_fini_list = make([]bool, c.Nmap)
	reduce_fini_list = make([]bool, c.Nreduce)
	maptask_worker = make(map[int]int64)
	reducetask_worker = make(map[int]int64)

	for i := 0; i < c.Nmap; i++ {
		map_fini_list[i] = false
		maptask_worker[i] = -1
	}
	for i := 0; i < c.Nreduce; i++ {
		reduce_fini_list[i] = false
		reducetask_worker[i] = -1
	}

	// start the process
	c.MapTask(files)

	c.server()

	return &c
}

// Integrate intermediate file data
func Integrate() {
	reduce_list = make([][]string, c.Nreduce)

	// read parent directory
	files, err := ioutil.ReadDir("../")
	if err != nil {
		log.Printf("read parent dir error, %v", err)
		return
	}

	// for each reduce task, find the file that would be used
	// and add it into the reduce_list
	for i := 0; i < c.Nreduce; i++ {
		for _, file := range files {
			// determine whether the suffix contains the task ID
			if strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				reduce_list[i] = append(reduce_list[i], file.Name())
			}
		}
	}
}

// produce map tasks to the workers
func (c *Coordinator) MapTask(files []string) {

	for i, filename := range files {
		// produce a task per file
		map_task := Task{
			Type:     State_Map,
			ID:       i,
			NReduce:  c.Nreduce,
			FileName: filename,
			FileList: nil,
		}

		// put the task into the chan
		// which can guarantee that one task only be received by one worker
		c.MapChan <- &map_task
	}
}

// produce reduce tasks to the workers
func (c *Coordinator) ReduceTask() {
	for i := 0; i < c.Nreduce; i++ {
		reduce_task := Task{
			Type:     State_Reduce,
			ID:       i,
			NReduce:  c.Nreduce,
			FileName: "",
			FileList: &reduce_list[i],
		}

		c.ReduceChan <- &reduce_task
	}
}

// send the signal of exit to workers
func (c *Coordinator) ExitTask() Task {
	exit_task := Task{
		Type:     State_Exit,
		ID:       0,
		NReduce:  c.Nreduce,
		FileName: "",
		FileList: nil,
	}

	return exit_task
}

func (c *Coordinator) NoTask() Task {
	task := Task{
		Type:     State_Free,
		ID:       0,
		NReduce:  c.Nreduce,
		FileName: "",
		FileList: nil,
	}
	return task
}

func Monitor_map(WorkerID int64, task *Task) {
	// sleep 10s
	time.Sleep(time.Duration(10) * time.Second)

	// if the worker haven't send the finish signal, we think it has collapsed
	mutex.Lock()         // lock
	defer mutex.Unlock() // unlock

	if maptask_worker[task.ID] == WorkerID && map_fini_list[task.ID] == false {
		// reset
		maptask_worker[task.ID] = -1
		// redo the task
		// put it into chan again
		c.MapChan <- task
	}
}

func Monitor_reduce(WorkerID int64, task *Task) {
	// sleep 10s
	time.Sleep(time.Duration(10) * time.Second)

	// if the worker haven't send the finish signal, we think it has collapsed
	mutex.Lock()         // lock
	defer mutex.Unlock() // unlock

	if reducetask_worker[task.ID] == WorkerID && reduce_fini_list[task.ID] == false {
		// reset
		reducetask_worker[task.ID] = -1
		// redo the task
		// put it into chan again
		c.ReduceChan <- task
	}
}
