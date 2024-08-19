package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Task struct {
	FileName string
	MapId    int
	ReduceId int
}

type Coordinator struct {
	// Your definitions here.
	State         int // map 0 reduce 1 Fin 2
	NumMapTask    int
	NumReduceTask int

	MapTask       chan Task
	ReduceTask    chan Task
	MapTaskFin    chan bool
	ReduceTaskFin chan bool
}

// type Coordinator struct {
// 	Y int
// }

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) CallExample(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 111
	return nil
}

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	// 提供给worker rpc 调用的
	// 需要先判断c的状态？
	if len(c.MapTaskFin) != c.NumMapTask {
		maptask, ok := <-c.MapTask
		if ok {
			reply.XTask = maptask
		}
	} else if len(c.ReduceTaskFin) != c.NumReduceTask {
		reducetask, ok := <-c.ReduceTask
		if ok {
			reply.XTask = reducetask
		}
	}
	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.State = c.State
	return nil
}

func (c *Coordinator) TaskFin(args *ExampleArgs, reply *ExampleReply) error {
	// 提供给worker rpc 调用的
	if len(c.MapTaskFin) != c.NumMapTask {
		c.MapTaskFin <- true
		if len(c.MapTaskFin) == c.NumMapTask {
			c.State = 1
		}
	} else if len(c.ReduceTaskFin) != c.NumReduceTask {
		c.ReduceTaskFin <- true
		if len(c.ReduceTaskFin) == c.NumReduceTask {
			c.State = 2
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.

	if len(c.ReduceTaskFin) == c.NumReduceTask {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:         0,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
		MapTaskFin:    make(chan bool, len(files)),
		ReduceTaskFin: make(chan bool, nReduce),
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
	}
	// 将文件名派发到task  一个文件对应一个task
	// Map的task。这里还可以使用NumMapTask作为id，即分桶进行负载均衡
	for id, file := range files {
		c.MapTask <- Task{FileName: file, MapId: id}
	}
	// Ruduce的task
	for i := 0; i < nReduce; i++ {
		c.ReduceTask <- Task{ReduceId: i}
	}
	// c := Coordinator{
	// 	Y: 111,
	// }
	// Your code here.
	c.server()
	return &c
}
