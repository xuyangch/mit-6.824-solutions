package mr

import (
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Master struct {
	// Your definitions here.
	jobs                []Job
	rawFiles            []string
	reportChannelByUUID sync.Map
	blockingJobNum      int
	availableJobs       chan Job
	successJobs         chan Job
	nReduce             int
	successJobsSet      map[string]bool
	isSuccess           bool
	mutex               sync.Mutex
	addReduce           bool
}

func (m *Master) handleSuccessJobs() {
	for {
		job, ok := <-m.successJobs
		if !ok {
			break
		}
		switch job.JobType {
		case MapJob:
			log.Println("new Map successJobs received!")
			log.Printf("len(m.successJobsSet) is %v", len(m.successJobsSet))
			log.Printf("len(job.FileNames) is %v", len(job.FileNames))
			taskIdentifier := strings.Split(job.FileNames[0], "-")[1]
			log.Printf("taskIdentifier is %v", taskIdentifier)
			if _, exist := m.successJobsSet[taskIdentifier]; !exist {
				log.Println("find a new taskIdentifier in success jobs")
				m.successJobsSet[taskIdentifier] = true
				if len(m.successJobsSet) == len(m.rawFiles) {
					m.mutex.Lock()
					defer m.mutex.Unlock()
					if m.addReduce {
						break
					}
					log.Println("All Map jobs succeed!")
					// add Reduce jobs
					log.Printf("before add reduce, len(m.availableJobs) is %v", len(m.availableJobs))
					for j := 0; j < m.nReduce; j++ {
						var fileNames []string
						for i := 0; i < len(m.rawFiles); i++ {
							taskIdentifier := strings.Split(m.rawFiles[i], "-")[1]
							fileNames = append(fileNames, "mr-"+taskIdentifier+"-"+strconv.Itoa(j))
						}

						m.availableJobs <- Job{
							JobType:   ReduceJob,
							FileNames: fileNames,
							NReduce:   m.nReduce,
						}
					}
					m.addReduce = true
					log.Printf("after add reduce, len(m.availableJobs) is %v", len(m.availableJobs))
				}
			}

		case ReduceJob:
			log.Println("new Reduce successJobs received!")
			for _, fileName := range job.FileNames {
				taskIdentifier := "reduce_" + strings.SplitN(fileName, "-", 2)[1]
				m.successJobsSet[taskIdentifier] = true
			}
			if len(m.successJobsSet) == len(m.rawFiles)*(m.nReduce+1) {
				log.Printf("All Success!")
				// mapreduce success
				close(m.availableJobs)
				close(m.successJobs)
				m.isSuccess = true
			}
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) GetTask(args *GetTaskArgs,
	reply *GetTaskReply) error {
	log.Println("A GetTask RPC received!")
	for {
		job, ok := <-m.availableJobs
		if !ok {
			log.Println("No more new tasks!")
			*reply = GetTaskReply{Job: Job{
				JobType: NoJob,
				NReduce: m.nReduce,
			}}
			return nil
		}
		reportChannel := make(chan Job)
		id := uuid.New().String()
		m.reportChannelByUUID.Store(id, reportChannel)
		*reply = GetTaskReply{Job: job, TaskId: id}
		go func() {
			log.Println("waiting for reportChannel to send job...")
			select {
			case job := <-reportChannel:
				log.Println("get job in reportChannel")
				log.Printf("len(job.FileNames) in reportChannel %v", len(job.FileNames))
				m.successJobs <- job

			case <-time.After(10 * time.Second):
				log.Println("timeout in reportChannel")
				m.availableJobs <- job
			}
		}()
		return nil

	}
}

func (m *Master) ReportSuccess(args *ReportSuccessArgs,
	reply *ReportSuccessReply) error {
	log.Println("A ReportSuccess call received")
	log.Printf("ReportSuccess job file length: %v", len(args.Job.FileNames))
	value, ok := m.reportChannelByUUID.Load(args.TaskId)
	if !ok {
		return errors.New("cannot read given uuid")
	}
	reportChanel := value.(chan Job)
	reportChanel <- args.Job
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
	return m.isSuccess
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.SetOutput(ioutil.Discard)
	m := Master{
		rawFiles:       files,
		blockingJobNum: nReduce,
		availableJobs:  make(chan Job, 100),
		successJobs:    make(chan Job, 100),
		nReduce:        nReduce,
		isSuccess:      false,
		successJobsSet: make(map[string]bool),
		addReduce:      false,
	}

	// Your code here.
	for _, fileName := range files {
		m.availableJobs <- Job{
			JobType:   MapJob,
			FileNames: []string{fileName},
			NReduce:   nReduce,
		}
	}
	go m.handleSuccessJobs()
	m.server()
	return &m
}
