package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	TaskName []string

	// MapWorkerState    []int
	// ReduceWorkerState []int
	MapTaskState    []int
	ReduceTaskState []int

	MapTaskAllDone    bool
	ReduceTaskAllDone bool

	NumberOfReduce int

	Mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForWorkIsDone(workerType *string, reply *bool) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// *reply = false
	if *workerType == "map" {
		*reply = c.MapTaskAllDone
	} else if *workerType == "reduce" {
		*reply = c.ReduceTaskAllDone
	}
	return nil
}

func (c *Coordinator) GetTask(workerType *string, worker *WorkerDetail) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	worker.WorkerState = -1
	if *workerType == "map" {
		if c.MapTaskAllDone {
			return nil
		}
		for i := 0; i < len(c.MapTaskState); i++ {
			if c.MapTaskState[i] == 0 {
				worker.WorkerType = "map"
				worker.WorkerState = 1
				worker.TaskID = i
				worker.TaskName = c.TaskName[i]
				worker.NumberOfMapWork = len(c.MapTaskState)
				worker.NumberOfReduceWork = c.NumberOfReduce
				c.MapTaskState[i] = 1
				break
			}
		}
	} else if *workerType == "reduce" {
		if c.ReduceTaskAllDone {
			return nil
		}
		assignReduceTask := false
		for i := 0; i < c.NumberOfReduce; i++ {
			if c.ReduceTaskState[i] == 0 {
				worker.WorkerType = "reduce"
				worker.WorkerState = 1
				worker.TaskID = i
				worker.TaskName = strconv.Itoa(i)
				worker.NumberOfMapWork = len(c.MapTaskState)
				worker.NumberOfReduceWork = c.NumberOfReduce
				c.ReduceTaskState[i] = 1
				assignReduceTask = true
				fmt.Printf("Coordinator assign reduce work %v\n", i)
				break
			}
		}
		if !assignReduceTask {
			fmt.Println("Coordinator donot assign reduce work")
		}
	}
	return nil
}

func (c *Coordinator) FinishTask(worker *WorkerDetail, reply *bool) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	worker.WorkerState = -1
	worker.TaskName = ""
	haveUnfinishTask := false
	if worker.WorkerType == "map" {
		c.MapTaskState[worker.TaskID] = 2
		*reply = true
		for i := 0; i < len(c.MapTaskState); i++ {
			if c.MapTaskState[i] != 2 {
				haveUnfinishTask = true
				break
			}
		}
		if !haveUnfinishTask {
			c.MapTaskAllDone = true
			fmt.Println("Map task are all finished!")
		}
		worker.WorkerState = 1
	} else {
		c.ReduceTaskState[worker.TaskID] = 2
		*reply = true
		for i := 0; i < len(c.ReduceTaskState); i++ {
			if c.ReduceTaskState[i] != 2 {
				haveUnfinishTask = true
				break
			}
		}
		if !haveUnfinishTask {
			c.ReduceTaskAllDone = true
			fmt.Println("Reduce task are all finished!")
		}
		worker.WorkerState = 1
	}
	return nil
}

func (c *Coordinator) SetMapTaskUnassign(TaskID *int, reply *bool) error {
	time.Sleep(10 * time.Second)
	fmt.Println("Map reset func begin")
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	*reply = false
	if c.MapTaskState[*TaskID] != 2 {
		c.MapTaskState[*TaskID] = 0
		*reply = true
		fmt.Printf("Map reset %v\n", *TaskID)
	} else {
		fmt.Printf("Map donot reset %v\n", *TaskID)
	}
	return nil
}

func (c *Coordinator) SetReduceTaskUnassign(TaskID *int, reply *bool) error {
	time.Sleep(10 * time.Second)
	fmt.Printf("Reduce reset %v func begin\n", *TaskID)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	*reply = false
	if c.ReduceTaskState[*TaskID] != 2 {
		c.ReduceTaskState[*TaskID] = 0
		*reply = true
		fmt.Printf("Reduce has reset %v\n", *TaskID)
	} else {
		fmt.Printf("Reduce donot reset %v\n", *TaskID)
	}
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

	// Your code here.
	if c.MapTaskAllDone && c.ReduceTaskAllDone {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	n := len(files)
	c := Coordinator{
		TaskName: files,

		// MapWorkerState:    make([]int, n),
		// ReduceWorkerState: make([]int, nReduce),
		MapTaskState:    make([]int, n),
		ReduceTaskState: make([]int, nReduce),

		NumberOfReduce: nReduce,

		MapTaskAllDone:    false,
		ReduceTaskAllDone: false,
	}

	// Your code here.

	c.server()
	return &c
}
