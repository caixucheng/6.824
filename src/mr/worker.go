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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	MapWork(mapf)

	// fmt.Println("Map workers shutdown")
	for !CallAskForWorkIsDone("map") {
		time.Sleep(2 * time.Second)
	}

	ReduceWork(reducef)

	// fmt.Println("Reduce workers shutdown")

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapWork(mapf func(string, string) []KeyValue) {
	for {
		if CallAskForWorkIsDone("map") {
			break
		}
		worker := CallGetTask("map")
		if worker.WorkerState == -1 {
			time.Sleep(3 * time.Second)
			continue
		} else {
			go CallSetMapTaskUnassign(worker.TaskID)
			fmt.Printf("Maper open the %v\n", worker.TaskName)
		}

		file, err := os.Open(worker.TaskName)
		if err != nil {
			log.Fatalf("cannot open %v", worker.TaskName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", worker.TaskName)
		}
		file.Close()

		kva := mapf(worker.TaskName, string(content))

		kvas := make([][]KeyValue, worker.NumberOfReduceWork)
		for _, kv := range kva {
			index := ihash(kv.Key) % worker.NumberOfReduceWork
			kvas[index] = append(kvas[index], kv)
		}
		for i := 0; i < worker.NumberOfReduceWork; i++ {
			intername := fmt.Sprintf("mr-%v-%v", worker.TaskID, i)
			interfile, _ := os.Create(intername)

			enc := json.NewEncoder(interfile)
			for _, kv := range kvas[i] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("Encode Intermidate error: ", err)
				}
			}
		}

		if CallFinishTask(worker) {
			fmt.Printf("Maper finished %v\n", worker.TaskName)
		} else {
			fmt.Printf("Maper unfinished %v\n", worker.TaskName)
		}
	}
}

func ReduceWork(reducef func(string, []string) string) {
	for {
		if CallAskForWorkIsDone("reduce") {
			break
		}
		worker := CallGetTask("reduce")
		if worker.WorkerState == -1 {
			fmt.Println("No reduce task to deal")
			time.Sleep(3 * time.Second)
			continue
		} else {
			go CallSetReduceTaskUnassign(worker.TaskID)
			fmt.Printf("Reducer open the mr-*-%v\n", worker.TaskID)
		}

		var kva []KeyValue
		for i := 0; i < worker.NumberOfMapWork; i++ {
			interFileName := fmt.Sprintf("mr-%v-%v", i, worker.TaskID)
			file, err := os.Open(interFileName)
			if err != nil {
				log.Fatalf("Reducer cannot open %v", interFileName)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}

		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf("mr-out-%v", worker.TaskID)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		ofile.Close()

		if CallFinishTask(worker) {
			fmt.Printf("Reducer finished mr-out-%v\n", worker.TaskID)
		} else {
			fmt.Printf("Reducer unfinished mr-out-%v\n", worker.TaskID)
		}
	}
}

func CallAskForWorkIsDone(workerType string) bool {
	done := false
	call("Coordinator.AskForWorkIsDone", workerType, &done)
	return done
}

func CallGetTask(workerType string) WorkerDetail {
	var worker WorkerDetail
	call("Coordinator.GetTask", workerType, &worker)
	return worker
}

func CallFinishTask(worker WorkerDetail) bool {
	ret := false
	call("Coordinator.FinishTask", &worker, &ret)
	return ret
}

func CallSetMapTaskUnassign(TaskID int) bool {
	ret := false
	call("Coordinator.SetMapTaskUnassign", &TaskID, &ret)
	return ret
}

func CallSetReduceTaskUnassign(TaskID int) bool {
	ret := false
	call("Coordinator.SetReduceTaskUnassign", &TaskID, &ret)
	return ret
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
