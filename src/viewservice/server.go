package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"


type ViewServer struct {
	mu                   sync.Mutex
	l                    net.Listener
	dead                 int32 // for testing
	rpccount             int32 // for testing
	me                   string

	// Your declarations here.
	currView             View                  //  keeps track of current view
	pingTimeMap          map[string]time.Time  //  keeps track of most recent time VS heard ping from each server
	primaryAckedCurrView bool                  //  keeps track of whether primary has acked the current view
	idleServer           string                //  keeps track of any idle servers
}


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.

	vs.pingTimeMap[args.Me] = time.Now()

	if args.Viewnum == 0 {
		if "" == vs.currView.Primary && "" == vs.currView.Backup {
			vs.changeView(args.Me, "")
			reply.View = vs.currView
			return nil
		} else {
			if vs.currView.Primary == args.Me {
				// Primary restarted. Proceeding to a new view
				vs.changeView(vs.currView.Backup, vs.getNewBackup())
			} else if vs.currView.Backup == args.Me {
				// Backup restarted. Proceeding to a new view
				vs.changeView(vs.currView.Primary, vs.getNewBackup())
			}
			reply.View = vs.currView
			return nil
		}
	}

	if !vs.primaryAckedCurrView {
		if args.Me == vs.currView.Primary && args.Viewnum == vs.currView.Viewnum {
			// the proceeding view is acknowledged
			vs.primaryAckedCurrView = true
		}
	}

	reply.View = vs.currView
	return nil
}


func (vs *ViewServer) getNewBackup() string {
	for k := range vs.pingTimeMap {
		if k != vs.currView.Primary && k != vs.currView.Backup {
			return k
		}
	}
	return ""
}
func (vs *ViewServer) newViewNum() {
	if vs.currView.Viewnum == ^uint(0) {
		vs.currView.Viewnum = 0
	} else {
		vs.currView.Viewnum++
	}
}

func (vs *ViewServer) changeView(p string, b string) bool {
	if vs.primaryAckedCurrView && (vs.currView.Primary != p || vs.currView.Backup != b) {
		vs.currView.Primary = p
		vs.currView.Backup = b
		vs.newViewNum()
		vs.primaryAckedCurrView = false
		return true
	}
	return false
}
//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.	

	// Add view to the reply message
	reply.View = vs.currView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.primaryAckedCurrView {
		for k, v := range vs.pingTimeMap {
			if time.Since(v) > DeadPings*PingInterval {
				delete(vs.pingTimeMap, k)
				if k == vs.currView.Primary {
					vs.changeView(vs.currView.Backup, vs.getNewBackup())
				} else if k == vs.currView.Backup {
					vs.changeView(vs.currView.Primary, vs.getNewBackup())
				}
			}
		}
		if vs.currView.Backup == "" {
			vs.changeView(vs.currView.Primary, vs.getNewBackup())
		}
		if vs.currView.Primary == "" {
			vs.changeView(vs.currView.Backup, vs.getNewBackup())
		}
	}
}


//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}


// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}


func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.pingTimeMap = make(map[string]time.Time)
	vs.primaryAckedCurrView = true
	vs.idleServer = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
