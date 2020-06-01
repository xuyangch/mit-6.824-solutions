package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type JobType int

const (
	NoJob     = iota
	MapJob    = iota
	ReduceJob = iota
)

type Job struct {
	JobType
	FileNames []string
	NReduce   int
}

// RPC definitions
type GetTaskArgs struct {
}

type GetTaskReply struct {
	Job
	TaskId string
}

type ReportSuccessArgs struct {
	Job
	TaskId string
}

type ReportSuccessReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
