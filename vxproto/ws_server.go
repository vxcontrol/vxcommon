package vxproto

import (
	"context"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/tomasen/realip"
)

// listenWS is only Server function
// TODO: here need to write of config argument description
func (vxp *vxProto) listenWS(config map[string]string) error {
	// TODO: here need to make more hardly validation for agentID
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/vxpws/agent/{id:[0-9a-z]+}/",
		func(w http.ResponseWriter, r *http.Request) {
			handleAgentWS(vxp, VXAgent, w, r)
		})
	r.HandleFunc("/api/v1/vxpws/browser/{id:[0-9a-z]+}/",
		func(w http.ResponseWriter, r *http.Request) {
			handleAgentWS(vxp, Browser, w, r)
		})
	// TODO: here need check wss scheme and make TLS server
	server := &http.Server{
		Addr:    config["socket"],
		Handler: r,
	}

	vxp.mutex.Lock()
	var closed bool
	var wg sync.WaitGroup
	closeChan := make(chan struct{})
	closeFunc := func() {
		if !closed {
			closed = true
			closeChan <- struct{}{}
		}
	}
	vxp.closers = append(vxp.closers, closeFunc)
	vxp.mutex.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-closeChan

		vxp.mutex.Lock()
		defer vxp.mutex.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		server.Shutdown(ctx)
		cancel()

		for idx, closer := range vxp.closers {
			if reflect.DeepEqual(closer, closeFunc) {
				// It's possible because there used break in the bottom
				vxp.closers = append(vxp.closers[:idx], vxp.closers[idx+1:]...)
				break
			}
		}
	}()

	err := server.ListenAndServe()
	closeFunc()
	wg.Wait()
	return err
}

func handleAgentWS(vxp *vxProto, agentType AgentType,
	w http.ResponseWriter, r *http.Request) {
	// Deny all but HTTP GET
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	// Upgrade connection to Websocket
	upgrader := websocket.Upgrader{
		// Use experimental feature
		EnableCompression: true,
		// TODO: May need to check the Origin but for now so
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Error Upgrading to websockets", 400)
		return
	}

	// Execute callback Connect if it was avalibale
	vars := mux.Vars(r)
	wsConn := &wsConnection{
		Conn: ws,
	}
	socket := &agentSocket{
		id:          vars["id"],
		ip:          getRealAddr(r),
		src:         vxp.NewToken(),
		at:          agentType,
		auth:        &AuthenticationData{},
		mRecv:       &sync.Mutex{},
		mSend:       &sync.Mutex{},
		IConnection: wsConn,
		IVaildator:  vxp,
		IProtoIO:    vxp,
	}
	if _, ok := vars["id"]; ok && vxp.IMainModule != nil {
		if err := vxp.OnConnect(socket); err != nil {
			http.Error(w, "Error Connection setup", 403)
			return
		}

		// Register new agent
		if !vxp.addAgent(socket) {
			http.Error(w, "Error Connection setup", 403)
			return
		}
		// Unregister agent after close connection
		defer vxp.delAgent(socket)
	} else {
		http.Error(w, "Error Connection setup", 403)
		return
	}

	// Run ping sender
	// TODO: fix it when there will able to support of ping packet on browser side
	if agentType == VXAgent {
		defer socket.runPingSender()()
	}

	// Read messages before connection will be closed
	for {
		err = socket.recvPacket()
		if err != nil {
			return
		}
	}
}

func isRealAddr(saddr string) bool {
	if strings.HasPrefix(saddr, "10.") || strings.HasPrefix(saddr, "192.168.") ||
		strings.HasPrefix(saddr, "172.") || strings.HasPrefix(saddr, "127.") ||
		strings.HasPrefix(saddr, "169.254.") || strings.HasPrefix(saddr, "::1") {
		return false
	}

	return true
}

func getRealAddr(r *http.Request) string {
	remoteAddrXFF := realip.FromRequest(r)
	remoteAddr := r.RemoteAddr
	if remoteAddrXFF != "" && !isRealAddr(remoteAddr) && isRealAddr(remoteAddrXFF) {
		_, port, _ := net.SplitHostPort(remoteAddr)
		remoteAddr = net.JoinHostPort(remoteAddrXFF, port)
	}

	return remoteAddr
}
