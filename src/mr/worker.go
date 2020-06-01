package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetOutput(ioutil.Discard)

	for {
		args := &GetTaskArgs{}
		reply := &GetTaskReply{}
		log.Println("Calling Master.GetTask...")
		call("Master.GetTask", args, reply)
		job := reply.Job
		if job.JobType == NoJob {
			break
		}
		switch job.JobType {
		case MapJob:
			log.Println("Get a Map job, working...")
			log.Printf("File names include: %v", job.FileNames[0])
			doMap(job, reply.TaskId, mapf)
		case ReduceJob:
			log.Println("Get a Reduce job, working...")
			log.Printf("File names include: %v, %v, %v", job.FileNames[0], job.FileNames[1], job.FileNames[2])
			doReduce(reply.Job, reply.TaskId, reducef)
		}

		// uncomment to send the Example RPC to the master.
		// CallExample()
	}
}

func doMap(job Job, taskId string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(job.FileNames[0])
	if err != nil {
		log.Fatalf("cannot open %v", job.FileNames)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", job.FileNames)
	}
	file.Close()
	kva := mapf(job.FileNames[0], string(content))
	sort.Sort(ByKey(kva))
	tmps := make([]*os.File, job.NReduce)
	for i := 0; i < job.NReduce; i++ {
		tmps[i], err = ioutil.TempFile("./", "temp_map_")
		if err != nil {
			log.Fatal("cannot create temp file")
		}
	}
	defer func() {
		for i := 0; i < job.NReduce; i++ {
			tmps[i].Close()
		}
	}()

	for _, kv := range kva {
		hash := ihash(kv.Key) % job.NReduce
		fmt.Fprintf(tmps[hash], "%v %v\n", kv.Key, kv.Value)
	}
	for i := 0; i < job.NReduce; i++ {
		taskIdentifier := strings.Split(job.FileNames[0], "-")[1]
		os.Rename(tmps[i].Name(), "mr-"+taskIdentifier+"-"+strconv.Itoa(i))
	}

	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Println("Job finishes, calling Master.ReportSuccess")
	log.Printf("JobType is %v", job.JobType)
	call("Master.ReportSuccess", newArgs, newReply)
}

func doReduce(job Job, taskId string, reducef func(string, []string) string) {
	kvas := make([][]KeyValue, len(job.FileNames))
	for i, fileName := range job.FileNames {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
			tokens := strings.Split(scanner.Text(), " ")
			kvas[i] = append(kvas[i], KeyValue{tokens[0], tokens[1]})
		}

	}
	ids := make([]int, len(job.FileNames))
	ofile, _ := ioutil.TempFile("./", "temp_reduce_")
	defer ofile.Close()
	values := []string{}
	prevKey := ""
	for {
		findNext := false
		var nextI int
		for i, kva := range kvas {
			if ids[i] < len(kva) {
				if !findNext {
					findNext = true
					nextI = i
				} else if strings.Compare(kva[ids[i]].Key, kvas[nextI][ids[nextI]].Key) < 0 {
					nextI = i
				}
			}
		}
		if findNext {
			nextKV := kvas[nextI][ids[nextI]]
			if prevKey == "" {
				prevKey = nextKV.Key
				values = append(values, nextKV.Value)
			} else {
				if nextKV.Key == prevKey {
					values = append(values, nextKV.Value)
				} else {
					fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
					prevKey = nextKV.Key
					values = []string{nextKV.Value}
				}
			}
			ids[nextI]++
		} else {
			break
		}
	}
	if prevKey != "" {
		fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
	}
	taskIdentifier := strings.Split(job.FileNames[0], "-")[2]
	os.Rename(ofile.Name(), "mr-out-"+taskIdentifier)

	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Println("Job finishes, calling Master.ReportSuccess")
	log.Printf("JobType is %v", job.JobType)
	call("Master.ReportSuccess", newArgs, newReply)
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
