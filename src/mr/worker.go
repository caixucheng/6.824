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
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				worker := WorkerDetail{
					WorkerType:     "map",
					WorkerID:       i,
					WorkerStatu:    0,
					TaskID:         0,
					TaskName:       "-1",
					NumberOfReduce: -1,
				}
				call("Coordinator.GetTask", "map", &worker)
				if worker.TaskName == "-1" {
					fmt.Println("No map task!")
					break
				}
				file, err := os.Open(worker.TaskName)
				if err != nil {
					log.Fatalf("Maper cannot open %v", worker.TaskName)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Maper cannot read %v", worker.TaskName)
				}
				file.Close()
				kva := mapf(worker.TaskName, string(content))

				kvas := make([][]KeyValue, worker.NumberOfReduce)
				for _, kv := range kva {
					index := ihash(kv.Key) % worker.NumberOfReduce
					kvas[index] = append(kvas[index], kv)
				}
				for i := 0; i < worker.NumberOfReduce; i++ {
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

				call("Coordinator.FinishTask", "map", &worker)
				if worker.WorkerStatu == 1 {
					fmt.Printf("Number %v maper finish the %v\n", worker.WorkerID, worker.TaskName)
				} else {
					log.Fatalf("Number %v maper unfinish the %v", worker.WorkerID, worker.TaskName)
				}

				time.Sleep(2 * time.Second)
			}
		}(i)
	}

	// time.Sleep(3 * time.Second)
	wg.Wait()

	// var intermediate []KeyValue
	// var output string
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				worker := WorkerDetail{
					WorkerType:     "reduce",
					WorkerID:       i,
					WorkerStatu:    0,
					TaskID:         0,
					TaskName:       "-1",
					NumberOfReduce: -1,
				}
				call("Coordinator.GetTask", "reduce", &worker)
				if worker.TaskName == "-1" {
					// log.Fatalf("Number %v reducer failed to get task!\n", i)
					fmt.Println("No reduce task!")
					break
				}

				var kva []KeyValue
				for index := 0; index < 8; index++ {
					intername := fmt.Sprintf("mr-%v-%v", index, worker.TaskName)
					// fmt.Println(intername)
					file, err := os.Open(intername)
					if err != nil {
						log.Fatalf("Reducer cannot open %v", intername)
					}

					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}

					file.Close()
				}

				sort.Sort(ByKey(kva))

				oname := fmt.Sprintf("mr-out-%v", worker.TaskName)
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

					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				call("Coordinator.FinishTask", "reduce", &worker)
				if worker.WorkerStatu == 1 {
					fmt.Printf("Number %v reducer finish the %v\n", worker.WorkerID, worker.TaskName)
				} else {
					log.Fatalf("Number %v reducer unfinish the %v", worker.WorkerID, worker.TaskName)
				}
			}
		}(i)
	}

	wg.Wait()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
