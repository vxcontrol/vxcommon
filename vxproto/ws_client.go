package vxproto

import (
	"errors"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// connectWS is only Client function. It's a blocking function
// TODO: here need config argument description
func (vxp *vxProto) connectWS(config map[string]string) error {
	agentType := "agent"
	if atype, ok := config["type"]; ok && atype == "browser" {
		agentType = atype
	}
	dialer := websocket.Dialer{}
	url := config["url"] + "/api/v1/vxpws/" + agentType + "/" + config["id"] + "/"
	ws, _, err := dialer.Dial(url, http.Header{})
	if err != nil {
		return err
	}

	// Use experimental feature
	ws.EnableWriteCompression(true)

	wsConn := &wsConnection{
		Conn: ws,
	}
	socket := &agentSocket{
		id:          config["id"],
		ip:          config["host"],
		src:         config["token"],
		at:          VXAgent,
		auth:        &AuthenticationData{},
		mRecv:       &sync.Mutex{},
		mSend:       &sync.Mutex{},
		IConnection: wsConn,
		IVaildator:  vxp,
		IProtoIO:    vxp,
	}
	if vxp.IMainModule != nil {
		if err := vxp.OnConnect(socket); err != nil {
			return errors.New("connection callback error: " + err.Error())
		}

		// Save received token
		config["token"] = socket.src

		// Register new agent
		if !vxp.addAgent(socket) {
			return errors.New("failed adding of agent")
		}
		// Unregister agent after close connection
		defer vxp.delAgent(socket)
	} else {
		return errors.New("disallowed unauthorized connection")
	}

	// Run ping sender
	defer socket.runPingSender()()

	// Read messages before connection will be closed
	for {
		err = socket.recvPacket()
		if err != nil {
			return err
		}
	}
}
