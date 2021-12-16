package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type Pair struct {
	Key   string
	Value string
	Op    string
}

type PBServer struct {
	mu           sync.Mutex
	l            net.Listener
	dead         int32 // for testing
	unreliable   int32 // for testing
	me           string
	vs           *viewservice.Clerk

	// Your declarations here.
	currView     viewservice.View  //  keeps track of current view
	database     map[string]string //  keeps track of the k/v database
	prevRequests map[int64]Pair    //  keeps track of requests sent by the client
	syncDatabase bool              //  keeps track of whether primary and backup DB's are in sync
}


//
// Process GET RPC requests from the client
// Primary should propagate updates to the backup server
//
func IsDupGet(pb *PBServer, args *GetArgs, reply *GetReply) bool {
	key, id := args.Key, args.Id
	prev, ok := pb.prevRequests[id]

	//  Duplicate RPC request
	if ok && prev.Key == key {
		return true
	}

	return false
}


func (pb *PBServer) ApplyGet(args *GetArgs, reply *GetReply) error {
	reply.Value= pb.database[args.Key]
	if reply.Value == "" {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}
	return nil
}


func (pb *PBServer) FwdGetToBackup(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	//  Should only perform update on the backup server
	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Update backup's database with Get operation
	pb.ApplyGet(args, reply)

	return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.currView.Primary == pb.me {
        if pb.currView.Backup != "" {
            var forwardReply GetReply
            for {
                ok := call(pb.currView.Backup, "PBServer.FwdGetToBackup", args, &forwardReply)
                if ok == false {
					retry := pb.updateView()
					if !retry {
						break
					}
                } else if forwardReply.Err != OK {
                    reply.Err = forwardReply.Err
                    return nil
                } else {
                    break
                }
            }
        }

		pb.ApplyGet(args, reply)
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}


//
// Process Put/Append RPC requests from the client
// Primary should propagate updates to the backup server
//
func IsDupPutAppend(pb *PBServer, args *PutAppendArgs, reply *PutAppendReply) bool {
	key, value, op, id := args.Key, args.Value, args.Op, args.Id
	prev, ok := pb.prevRequests[id]

	//  Duplicate RPC request
	if ok && prev.Key == key && prev.Value == value && prev.Op == op {
		return true
	}

	return false
}


func (pb *PBServer) ApplyPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	key, value := args.Key, args.Value 
	if args.Op == "Put" {
		pb.database[key] = value
	} else if args.Op == "Append" {
		prev := pb.database[key]
		pb.database[key] = prev + value
	}

	//  Update prevRequests map
	pb.prevRequests[args.Id] = Pair{Key: args.Key, Value: args.Value, Op: args.Op}

	reply.Err = OK
	return nil
}


func (pb *PBServer) FwdPutAppendToBackup(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	//  Should only perform update on the backup server
	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Check if we have seen this request before
	if IsDupPutAppend(pb, args, reply) {
		reply.Err = OK
		return nil
	}

	//  Update backup's database with Put/Append operation
	pb.ApplyPutAppend(args, reply)

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.currView.Primary == pb.me {
        if pb.currView.Backup != "" {
            var forwardReply PutAppendReply
            for {
                ok := call(pb.currView.Backup, "PBServer.FwdPutAppendToBackup", args, &forwardReply)
                if ok == false {
					retry := pb.updateView()
					if !retry {
						break
					}
                } else if forwardReply.Err != OK {
                    reply.Err = forwardReply.Err
                    return nil
                } else {
                    break
                }
            }
        }
		if IsDupPutAppend(pb, args, reply){
			reply.Err = OK
			return nil
		}
		pb.ApplyPutAppend(args, reply)
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) FwdDatabaseToBackup(args *FwdDatabaseToBackupArgs, 
	reply *FwdDatabaseToBackupReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	
	newView, err := pb.vs.Ping(pb.currView.Viewnum)
	if err != nil {
		fmt.Errorf("Ping(%v) failed", pb.currView.Viewnum)
	}
	//  Ping viewservice for current view
	//  Should only perform update on the backup server
	if newView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Update backup's database
	pb.database = args.Database
	pb.prevRequests = args.PrevRequests

	reply.Err = OK
	return nil
}

func (pb *PBServer) updateView() bool {
	newView, err := pb.vs.Ping(pb.currView.Viewnum)
	//log.Printf("Pinged with view [%d]", viewnum)
	res := true

	switch {
	case err != nil:
		// viewserver has crashed
		//log.Fatal("Ping error: ", err)
		pb.currView = viewservice.View{0, "", ""}
	case pb.currView.Viewnum == newView.Viewnum:
		// if view has not changed nothing needs to happen
	case pb.me == newView.Primary && newView.Backup == "":
		// if view has changed and I am now primary
		// with no backup
		pb.currView = newView
		pb.vs.Ping(pb.currView.Viewnum)
		res = false
	case pb.me == newView.Primary:
		e := pb.syncToBackup(newView.Backup)
		if e == nil {
			//log.Printf("Sync Success")
			pb.currView = newView
			pb.vs.Ping(pb.currView.Viewnum)
		}
	case pb.me == newView.Backup:
		pb.currView = newView
		res = false
	default:
		res = false
	}

	return res
}
func (pb *PBServer) syncToBackup(server string) error {
    args := &FwdDatabaseToBackupArgs{}
    args.Database = pb.database
    args.PrevRequests = pb.prevRequests
    var reply FwdDatabaseToBackupReply

    ok := call(server, "PBServer.FwdDatabaseToBackup", args, &reply)
    if ok == false || reply.Err != OK {
        //log.Printf("Server sync to backup failed")
        return fmt.Errorf("Sync failed.")
    }
    return nil
}
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.updateView()
}


// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}


// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}


// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currView = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.database = make(map[string]string)
	pb.prevRequests = make(map[int64]Pair)
	pb.syncDatabase = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}