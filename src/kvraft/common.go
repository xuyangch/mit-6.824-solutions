package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Id    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Id  int64

	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
