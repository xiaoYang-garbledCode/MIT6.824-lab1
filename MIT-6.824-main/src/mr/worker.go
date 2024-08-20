package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//Your worker implementation here.
	for {
		args := TaskRequest{}
		reply := TaskResponse{}
		CallGetTask(&args, &reply)
		// time.Sleep(time.Second)
		state := reply.State
		CurNumMapTask := reply.CurNumMapTask
		CurNumReduceTask := reply.CurNumReduceTask
		if CurNumMapTask >= 0 && state == 0 {
			filename := reply.XTask.FileName
			id := reply.XTask.MapId
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open mapTask %v", filename) // %v
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename) // %v
			}
			file.Close()
			kva := mapf(filename, string(content))
			num_Reduce := reply.NumReduceTask
			// 将kva按照key的ihash值放入bucket中
			bucket := make([][]KeyValue, num_Reduce)
			for _, kv := range kva {
				num := ihash(kv.Key) % num_Reduce
				bucket[num] = append(bucket[num], kv)
			}
			// 遍历bucket将bucket的所有内容写入到intermediate文件中
			for i := 0; i < num_Reduce; i++ {
				// 创建temp文件
				tmp_file, error := os.CreateTemp("", "mr-map-*")
				if error != nil {
					log.Fatalf("cannot open tmp_file")
				}
				// 使用json将内容写入文件
				enc := json.NewEncoder(tmp_file)
				err := enc.Encode(bucket[i])
				if err != nil {
					log.Fatalf("encode bucket error")
				}
				tmp_file.Close()
				// 临时文件写好之后，编码也完成更名为最终的intermediate文件
				out_file_name := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
				os.Rename(tmp_file.Name(), out_file_name)
			}
			// 走到这是map任务完成了，需要将coordinator发送完成的请求，即直接修改reply
			CallTaskFin()
		} else if CurNumReduceTask >= 0 && state == 1 {
			// filename=""为空，说明此时的reply里没有Task了
			num_map := reply.NumMapTask
			id := strconv.Itoa(reply.XTask.ReduceId)
			intermediate := []KeyValue{}
			// 如果len的MapTaskfin全部完成了？则可以开始进行num_map
			// do Reduce
			for i := 0; i < num_map; i++ {
				map_filename := "mr-" + strconv.Itoa(i) + "-" + id
				// 打开intermediate文件
				inputFile, err := os.OpenFile(map_filename, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reduceTask %v", map_filename)
				}
				// 解码，将结果都存入中
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}
			sort.Sort(ByKey(intermediate))
			out_file_name := "mr-out-" + id
			tmp_file, err := os.CreateTemp("", "mr-reduce-*")
			if err != nil {
				log.Fatalf("cannot open tmp_file")
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(tmp_file, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			tmp_file.Close()
			os.Rename(tmp_file.Name(), out_file_name)
			CallTaskFin()
		} else {
			break
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 101

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.CallExample", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetTask(args *TaskRequest, reply *TaskResponse) {
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("call GetMapTask ok! \n")
	} else {
		fmt.Printf("call failed! \n")
	}
}

func CallTaskFin() {
	// send the RPC request, wait for the reply.
	args := ExampleArgs{}
	reply := ExampleReply{}
	ok := call("Coordinator.TaskFin", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("call GetTaskFin ok! \n")
	} else {
		fmt.Printf("call failed! \n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
