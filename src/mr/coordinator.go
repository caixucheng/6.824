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
)

type Coordinator struct {
	// Your definitions here.
	TaskName []string

	NumberOfTask    int
	NumberOfMaper   int
	NumberOfReducer int

	MaperStatu      []int
	MapTaskStatu    []int
	ReducerStatu    []int
	ReduceTaskStatu []int

	mapAllDone    bool
	reduceAllDone bool

	quit      chan bool
	TaskMutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(workerType *string, worker *WorkerDetail) error {
	c.TaskMutex.Lock()
	if *workerType == "map" {
		mapAllDone := true
		for i := 0; i < c.NumberOfTask; i++ {
			if c.MapTaskStatu[i] == 0 {
				*worker = WorkerDetail{
					WorkerType:     "map",
					WorkerStatu:    2,
					TaskID:         i,
					TaskName:       c.TaskName[i],
					NumberOfReduce: c.NumberOfReducer,
				}
				c.MapTaskStatu[i] = 1
				c.MaperStatu[i] = 2
				mapAllDone = false
				break
			}
		}
		if mapAllDone {
			fmt.Println("Map Tasks are finished!")
		}
	} else {
		reduceAllDone := true
		for i := 0; i < c.NumberOfReducer; i++ {
			if c.ReduceTaskStatu[i] == 0 {
				*worker = WorkerDetail{
					WorkerType:     "reduce",
					WorkerStatu:    2,
					TaskID:         i,
					TaskName:       strconv.Itoa(i),
					NumberOfReduce: c.NumberOfReducer,
				}
				c.ReduceTaskStatu[i] = 1
				c.ReducerStatu[i] = 2
				reduceAllDone = false
				break
			}
		}
		if reduceAllDone {
			fmt.Println("Reduce Tasks are finished!")
			c.quit <- true
		}
	}
	c.TaskMutex.Unlock()
	return nil
}

func (c *Coordinator) FinishTask(workerType *string, worker *WorkerDetail) error {
	if *workerType == "map" {
		*worker = WorkerDetail{
			WorkerType:  "map",
			WorkerStatu: 1,
			TaskID:      0,
			// TaskName:       "-1",
			NumberOfReduce: 0,
		}
		c.MapTaskStatu[worker.TaskID] = 2
		c.MaperStatu[worker.WorkerID] = 1
	} else {
		*worker = WorkerDetail{
			WorkerType:  "reduce",
			WorkerStatu: 1,
			TaskID:      0,
			// TaskName:       "-1",
			NumberOfReduce: 0,
		}
		c.ReduceTaskStatu[worker.TaskID] = 2
		c.ReducerStatu[worker.WorkerID] = 1
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := true

	// Your code here.
	ret = <-c.quit

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

		NumberOfTask:    n,
		NumberOfMaper:   n,
		NumberOfReducer: nReduce,

		MapTaskStatu:    make([]int, n),
		MaperStatu:      make([]int, n),
		ReduceTaskStatu: make([]int, nReduce),
		ReducerStatu:    make([]int, nReduce),

		mapAllDone:    false,
		reduceAllDone: false,

		quit: make(chan bool),
	}

	// Your code here.
	defer fmt.Println("Coordinator is running!")

	c.server()
	return &c
}
