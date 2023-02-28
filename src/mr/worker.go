package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"

	idworker "github.com/gitstliu/go-id-worker"
)

// the message of the worker
type Worker_ struct {
	WorkerID int64 // the identify of the worker which sends request
	state    int   // current state: free, map or reduce
}

// define a worker
var worker_ Worker_

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// the pretreatment before calling map which is defined by users
func mapProcess(mapf func(string, string) []KeyValue, reducef func(string, []string) string,
	filename string, task_id int, reduce_num int) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// call the map function
	kva := mapf(filename, string(content))

	// Hash key-value pairs
	HashKv := make([][]KeyValue, reduce_num)
	for _, kv := range kva {
		HashKv[ihash(kv.Key)%reduce_num] = append(HashKv[ihash(kv.Key)%reduce_num], kv)
	}

	// fmt.Println(HashKv)

	// save the key/value pairs in the format of json
	// for every reduce_file
	for i := 0; i < reduce_num; i++ {
		tempFile, err := ioutil.TempFile("", "temp*-"+strconv.Itoa(task_id)+"-"+strconv.Itoa(i))
		if err != nil {
			log.Printf("map create temp file error, %v", err)
			return
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range HashKv[i] {
			err := enc.Encode(&kv)

			// ignore the error
			if err != nil {
				continue
			}
		}

		// rename the tempfile and store it
		err = os.Rename(tempFile.Name(), "../mr-"+strconv.Itoa(task_id)+"-"+strconv.Itoa(i))
		if err != nil {
			log.Printf("rename file error, %v", err)
			tempFile.Close()
			return
		}
		tempFile.Close()
	}

	// todo
	// log.Printf("the worker %v finish the map task%v", worker_.WorkerID, task_id)

	// send the success msg
	go SendFinish(State_Map, task_id)

	// change worker state
	worker_.state = State_Free

	// resent requests to coordinator
	Cycle_req(mapf, reducef)
}

// the pretreatment before calling reduce which is defined by users
func reduceProcess(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task_id int, filelist *[]string) {

	// store the key/value pairs
	kva := make([]KeyValue, 0)

	for _, filename := range *filelist {
		// open the file
		file, err := os.Open("../" + filename)
		if err != nil {
			log.Printf("open reduce file %v error, %v", filename, err)
			return
		}

		// read the file with json formal
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// sort the key/value pairs
	sort.Sort(ByKey(kva))

	// output the res
	ofile, err := ioutil.TempFile("", "temp*-"+strconv.Itoa(task_id))
	if err != nil {
		log.Printf("create output temp file error, %v", err)
		return
	}

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// rename the tempfile and store it
	err = os.Rename(ofile.Name(), "mr-out-"+strconv.Itoa(task_id))
	if err != nil {
		log.Printf("rename file error, %v", err)
		ofile.Close()
		return
	}
	ofile.Close()

	// todo
	// log.Printf("the worker %v finish the reduce task%v", worker_.WorkerID, task_id)

	// send the success msg
	go SendFinish(State_Reduce, task_id)

	// change worker state
	worker_.state = State_Free

	// resent requests to coordinator
	Cycle_req(mapf, reducef)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// init
	// at this time it is free

	// assign a unique id
	current_worker := &idworker.IdWorker{}
	current_worker.InitIdWorker(1000, 1)
	newID, err := current_worker.NextId()
	if err != nil {
		log.Fatalf("cannot assign a unique id")
	}

	worker_.WorkerID = newID
	worker_.state = State_Free

	// log.Printf("worker %v init successfully", worker_.WorkerID)

	// start to send request
	// log.Printf("worker %v begin to send request", worker_.WorkerID)
	Cycle_req(mapf, reducef)
}

// resend request after the task finished or at first
func Cycle_req(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// send request for tasks
	task := AskTask()

	// fmt.Println(task)

	switch task.Type {

	case State_Map:
		// log.Printf("worker %v start map task%v", worker_.WorkerID, task.ID)
		// change the state of the worker
		worker_.state = State_Map
		// call mapProcess
		mapProcess(mapf, reducef, task.FileName, task.ID, task.NReduce)

	case State_Reduce:
		// log.Printf("worker %v start reduce task%v", worker_.WorkerID, task.ID)
		// change the state of the worker
		worker_.state = State_Map
		// call reduceProcess
		reduceProcess(mapf, reducef, task.ID, task.FileList)

	case State_Exit:
		log.Printf("worker %v exit", worker_.WorkerID)
		os.Exit(0)

	default:
		Cycle_req(mapf, reducef)
	}
}

func SendFinish(task_type int, task_id int) {
	args := Task_Finish{
		TaskType: task_type,
		TaskID:   task_id,
		WorkerID: worker_.WorkerID,
	}

	reply := Success{}

	call("Coordinator.CheckSuccess", &args, &reply)
}

// send coordinator a request for task
func AskTask() *Task {
	// declare an argument structure
	// and fill the msg
	args := Req{
		WorkerID: worker_.WorkerID,
	}

	// declare a reply structure
	reply := Task{}

	// log.Printf("worker %v send the RPC request", worker_.WorkerID)

	// send the RPC request and wait for the reply
	call("Coordinator.Handler", &args, &reply)

	return &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
