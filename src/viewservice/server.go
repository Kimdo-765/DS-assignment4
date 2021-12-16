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
	primaryAckedCurrView bool                  //  keeps track of whether primary has ACKed the current view
}


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.	

	// 1. Update ping times for current server
	
	// 2. Update view and/or idle server if reboot or new server, or ACK the current view

	vs.pingTimeMap[args.Me] = time.Now()
	if vs.currView.Viewnum == 0 { //initiate
		vs.updateView(args.Me, "")
	}
	if vs.primaryAckedCurrView == false { //primary에 의해 승인되지 않은 view일경우
		if args.Me == vs.currView.Primary && args.Viewnum == vs.currView.Viewnum {
			vs.primaryAckedCurrView  = true
		}
	}

	if args.Me == vs.currView.Primary {//메시지를 primary가 보냈는지
		if vs.hasCrashed(args) { //고장
			if vs.currView.Backup !=  "" { //backup이 있는지
				if vs.isAlive(vs.currView.Backup) { //Backup이 응답하는지
					nextBackup := vs.getNextServer()
					vs.updateView(vs.currView.Backup, nextBackup)
				}
			} else { // 고장인데 backup이 없음 => critical error
				return nil
			}
		}
	} else if args.Me != vs.currView.Backup { // backup이 아닌 idle일 경우
		if vs.isAlive(vs.currView.Primary) && vs.currView.Backup == ""{ //Primary는 살아있는데 Backup이 없을 경우
			vs.updateView(vs.currView.Primary, args.Me)
		}
	}
	reply.View = vs.currView

	return nil
}

func (vs *ViewServer) hasCrashed(args *PingArgs) bool {
	return vs.currView.Viewnum > 1 && args.Viewnum == 0
}

func (vs *ViewServer) isAlive(name string) bool {
	pingTime := vs.pingTimeMap[name]

	now := time.Now()
	return now.Sub(pingTime) < PingInterval * DeadPings
}

func (vs *ViewServer) getNextServer() string {
	for next := range vs.pingTimeMap {
		if vs.isAlive(next) && next != vs.currView.Primary && next != vs.currView.Backup {
			return next
		}
	}
	return ""
}

func (vs *ViewServer) updateView(primary string, backup string) { // view change
	// log.Printf("updating to view {%d, %s, %s}", vs.currView.Viewnum + 1, primary, backup)
	vs.currView = View{vs.currView.Viewnum + 1, primary, backup}
	vs.primaryAckedCurrView = false
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
	vs.mu.Lock()
	defer vs.mu.Unlock()	

	// Your code here.	

	// 1. No recent pings from the idle server

	// 2. No recent pings from the backup

	// 3. No recent pings from the primary
	if vs.primaryAckedCurrView == true {
		if vs.isAlive(vs.currView.Primary){ //primary가 활성화되어있을때
			if vs.currView.Backup == ""{ //backup은 없을때
				nextBackup := vs.getNextServer()
				if nextBackup != "" { //다음 서버가 있다면
					vs.updateView(vs.currView.Primary, nextBackup) //그 서버를 backup으로 업데이트
				}
			} else { //backup이 있을때
				if !vs.isAlive(vs.currView.Backup) {//backup이 죽었으면
					nextBackup := vs.getNextServer()
					vs.updateView(vs.currView.Primary, nextBackup) //다음서버로 backup설정
				}
			}
		} else { //primary가 죽었을때
			if vs.currView.Backup == ""{//backup이 없다면
				return // critical
			} else {
				if vs.isAlive(vs.currView.Backup) { // backup이 활성화
					nextBackup := vs.getNextServer()
					vs.updateView(vs.currView.Backup, nextBackup) //backup을 primary 다음서버를 backup으로
				} else {
					return // critical
				}
			}
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
	vs.primaryAckedCurrView = false

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