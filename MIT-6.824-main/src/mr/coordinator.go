package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	FileName string
	MapId    int
	ReduceId int
}

type Coordinator struct {
	// Your definitions here.
	State          int32 // map 0 reduce 1 Fin 2
	NumMapTask     int
	NumReduceTask  int
	MapTask        chan Task
	ReduceTask     chan Task
	MapTaskTime    sync.Map
	ReduceTaskTime sync.Map
	files          []string
}

type TimeStamp struct {
	Time int64
	Fin  bool
}

func lenSyncFin(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	return i
}

// 计算 sync.Map 中 TimeStamp 结构体的 Fin 字段为 true 的数量。
func lenTaskFin(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		if v.(TimeStamp).Fin {
			i++
		}
		return true
	})
	return i
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
	if len(c.MapTask) != 0 {
		// fmt.Println(">>>>>>>>>>>>>>>MapTask: ", len(c.MapTask))
		maptask, ok := <-c.MapTask
		if ok {
			reply.XTask = maptask
		}
		reply.CurNumMapTask = len(c.MapTask)
		reply.CurNumReduceTask = len(c.ReduceTask)
	} else {
		reply.CurNumMapTask = -1
		reply.CurNumReduceTask = len(c.ReduceTask)
	}
	if c.State == 1 {
		if len(c.ReduceTask) != 0 {
			// fmt.Println(">>>>>>>>>>>>>>>ReduceTask: ", len(c.ReduceTask))
			reducetask, ok := <-c.ReduceTask
			if ok {
				reply.XTask = reducetask
			}
			reply.CurNumMapTask = -1
			reply.CurNumReduceTask = len(c.ReduceTask)
		} else {
			reply.CurNumMapTask = -1
			reply.CurNumReduceTask = -1
		}
	}
	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.State = c.State
	return nil
}

func (c *Coordinator) TaskFin(args *Task, reply *ExampleReply) error {
	// 提供给worker rpc 调用的
	time_now := time.Now().Unix()
	if lenTaskFin(&c.MapTaskTime) != c.NumMapTask {
		start_time, _ := c.MapTaskTime.Load(args.MapId)
		if time_now-start_time.(TimeStamp).Time > 10 {
			return nil
		}
		c.MapTaskTime.Store(args.MapId, TimeStamp{time_now, true})
		if lenTaskFin(&c.MapTaskTime) == c.NumMapTask {
			atomic.StoreInt32(&c.State, 1)
			for i := 0; i < c.NumReduceTask; i++ {
				c.ReduceTask <- Task{ReduceId: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	} else if lenTaskFin(&c.ReduceTaskTime) != c.NumReduceTask {
		start_time, _ := c.ReduceTaskTime.Load(args.MapId)
		if time_now-start_time.(TimeStamp).Time > 10 {
			return nil
		}
		c.ReduceTaskTime.Store(args.ReduceId, TimeStamp{time_now, true})
		if lenTaskFin(&c.ReduceTaskTime) == c.NumReduceTask {
			atomic.StoreInt32(&c.State, 2)
		}
	}
	return nil
}

func (c *Coordinator) TimeTick() {
	state := atomic.LoadInt32(&c.State)
	time_now := time.Now().Unix()
	if state == 0 {
		for i := 0; i < c.NumMapTask; i++ {
			tmp, _ := c.MapTaskTime.Load(i)
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("map time out!")
				c.MapTask <- Task{FileName: c.files[i], MapId: i}
				c.MapTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	} else if state == 1 {
		for i := 0; i < c.NumReduceTask; i++ {
			tmp, _ := c.ReduceTaskTime.Load(i)
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("reduce time out!")
				c.ReduceTask <- Task{ReduceId: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	}
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
	// ret := false
	// Your code here.
	var ret bool
	if c.State == 2 {
		ret = true
	} else {
		ret = false
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
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		files:         files,
	}
	time_now := time.Now().Unix()
	// 将文件名派发到task  一个文件对应一个task
	// Map的task。这里还可以使用NumMapTask作为id，即分桶进行负载均衡
	for id, file := range files {
		c.MapTask <- Task{FileName: file, MapId: id}
		c.MapTaskTime.Store(id, TimeStamp{time_now, false})
	}
	c.server()
	return &c
}
