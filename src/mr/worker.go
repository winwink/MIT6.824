package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "os"
import "io/ioutil"
import "sort"
import "regexp"
import "strings"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type KeyValues struct {
	Values []KeyValue
}

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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	GetTask(mapf, reducef)
}

func GetTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string){
	args := ExampleArgs{}
	reply := TaskState{}
	call("Master.GetTask", args, &reply)
	if(reply.TaskName==""){
		fmt.Println("No Task")
		return;
	} else {
		fmt.Println("GetTask "+reply.ToString2())
	}
	if(reply.TaskType=="Map"){
		GetTaskMap(mapf, reply)
	} else {
		GetTaskReduce(reducef, reply)
	}

	UpdateTask(reply)
}

func GetTaskMap(mapf func(string, string) []KeyValue, reply TaskState){
	filename := reply.TaskName
	file, err := os.Open(reply.TaskName)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// fmt.Println("Read file success", content)
	kva := mapf(filename, string(content))

	keyvalues := []KeyValues{}
	for i:=0;i<reply.NReduce;i++{
		keyvalue := []KeyValue{}
		k := KeyValues{}
		k.Values = keyvalue
		keyvalues = append(keyvalues, k)
	}
	for _,kv := range kva{
		hashNo := ihash(kv.Key)
		nReduce := hashNo % reply.NReduce
		// fmt.Println("hash, key:"+kv.Key+",hashNo:"+strconv.Itoa(hashNo)+",nReduce:"+strconv.Itoa(nReduce))
		keyvalues[nReduce].Values = append(keyvalues[nReduce].Values, kv)
	}
	for i:=0;i<len(keyvalues);i++{
		kvs := keyvalues[i]
		oname := "mr-"+strconv.Itoa(reply.TaskNo)+"-"+strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		sort.Sort(ByKey(kvs.Values))
		for _,kv := range kvs.Values{
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}
}

func GetTaskReduce(reducef func(string, []string) string, reply TaskState){
	reg := regexp.MustCompile(`^mr-\d+-`+strconv.Itoa(reply.TaskNo)+`$`)
	fmt.Println("regex:"+reg.String())
	files, _ := ioutil.ReadDir(`./`)
	kva := []KeyValue{}
	for _, file:= range files{
		if file.IsDir(){
			continue
		} else {
			filename:= file.Name()
			fmt.Println(filename)
			if(reg.MatchString(filename)){
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				// fmt.Println(string(content))
				lines :=strings.Split(string(content), "\n")
				for _,line := range lines{
					// fmt.Println("line:"+line)
					items := strings.Split(line, " ")
					if(len(items)==2){
						kv := KeyValue{items[0], items[1]}
						kva = append(kva, kv)
					}
				}
			}
		}
		fmt.Println("kva len:"+strconv.Itoa(len(kva)))
	}
	sort.Sort(ByKey(kva))

	oname := "mr-out-"+strconv.Itoa(reply.TaskNo)
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
}
func UpdateTask(task TaskState){
	reply := TaskState{}
	call("Master.UpdateTask", task, &reply)
}

func (task *TaskState) ToString2() string{
	return "Task, Type:"+task.TaskType+", Name:"+task.TaskName+", No:"+strconv.Itoa(task.TaskNo)+", State: "+strconv.Itoa(task.State)
}
//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
