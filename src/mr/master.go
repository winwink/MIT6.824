package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "fmt"
import "sync"
import "time"


type Master struct {
	// Your definitions here.
	MapTask []TaskState
	ReduceTask []TaskState
	NReduce int
	Mutex sync.Mutex
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *ExampleArgs, reply *TaskState) error {
	//fmt.Println("GetTask Start")
	reply.AllJobDone = false
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	// index := CheckTimeoutTask(m.MapTask)
	// if(index!=-1){
	// 	m.MapTask[index].State = 0
	// }

	// index = CheckTimeoutTask(m.ReduceTask)
	// if(index!=-1){
	// 	m.ReduceTask[index].State=0
	// }
	mapTaskDone := m.MapTaskDone()
	if(mapTaskDone!=true){
		task := GetFirstTaskUnsigned(m.MapTask)
		if(task==nil) {
			return nil
		}
		fmt.Println("GetTask Map, Task:"+task.ToString())
		task.AssignTime = time.Now()
		reply.CopyFrom(task)
		m.MapTask[task.TaskNo].State = 1
		return nil
	}
	
	reduceTaskDone := m.ReduceTaskDone()
	if(reduceTaskDone != true){
		task := GetFirstTaskUnsigned(m.ReduceTask)
		if(task==nil) {
			return nil
		}
		fmt.Println("GetTask Reduce, Task"+task.ToString())
		task.AssignTime = time.Now()
		reply.CopyFrom(task)
		m.ReduceTask[task.TaskNo].State = 1
		return nil
	}
	fmt.Println("GetTask Done")
	reply.AllJobDone = true
	
	return nil
}

func (m *Master) UpdateTask(task TaskState, reply *TaskState) error{
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	//fmt.Println("Update Task Type:"+task.TaskType+", No:"+ strconv.Itoa(task.TaskNo))
  if(task.TaskType=="Map"){ 
		//fmt.Println("Update Task map match")
		m.MapTask[task.TaskNo].State = 2
		return nil
	}
	
	if(task.TaskType=="Reduce"){
		//fmt.Println("Update Task reduce match")
		m.ReduceTask[task.TaskNo].State = 2
		return nil
	}
	fmt.Println("Update Task none match")
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	for _,v := range m.MapTask{
		if(v.State != 2){
			return ret
		}
	}
	for _,v := range m.ReduceTask{
		if(v.State != 2){
			return ret;
		}
	}
	ret = true
	return ret
}

func (m *Master) MapTaskDone() bool{
	for _,v := range m.MapTask{
		if(v.State != 2){
			return false
		}
	}
	return true
}

func (m *Master) ReduceTaskDone() bool{
	ret := false
	for _,v := range m.ReduceTask{
		if(v.State != 2){
			return ret
		}
	}
	return true
}

func CheckTimeoutTask(tasks []TaskState) int{
  for _,task := range tasks{
		if(task.State == 1 && task.AssignTime.Add(time.Second * 10).Before(time.Now()) ){
			fmt.Println("task timeout"+strconv.Itoa(task.TaskNo))
			task.State = 0
			return task.TaskNo
		}
	}
	return -1
}

func GetFirstTaskUnsigned(tasks []TaskState) *TaskState{
  for _,task := range tasks{
		if(task.State == 1 && task.AssignTime.Add(time.Second * 10).Before(time.Now())){
			fmt.Println("task timeout"+strconv.Itoa(task.TaskNo))
			return &task
		}
		if(task.State == 0){
			return &task
		}
	}
	return nil
}

func (task *TaskState) ToString() string{
	return "Task, Type:"+task.TaskType+", Name:"+task.TaskName+", No:"+strconv.Itoa(task.TaskNo)+", State: "+strconv.Itoa(task.State)
}

func (task *TaskState) CopyFrom(s *TaskState){
	task.TaskName = s.TaskName
	task.TaskNo = s.TaskNo
	task.TaskType = s.TaskType
	task.State = s.State
	task.NReduce = s.NReduce
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.MapTask = []TaskState{}
	now := time.Now()
	for i:=0;i<len(files);i++ {
		task := TaskState{files[i], "Map", i, 0, nReduce, false, now}
		m.MapTask = append(m.MapTask, task)
	}

	m.ReduceTask = []TaskState{}
	for i:=0;i<nReduce;i++{
		task := TaskState{strconv.Itoa(i), "Reduce", i, 0, nReduce, false, now}
		m.ReduceTask = append(m.ReduceTask, task)
	}

	m.NReduce = nReduce

	m.server()
	return &m
}
