package vxproto

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/vxcontrol/vxcommon/agent"
)

// IAgentSocket is main interface for Agent Socket integration
type IAgentSocket interface {
	GetPublicInfo() *AgentInfo
	GetSource() string
	SetSource(src string)
	GetDestination() string
	SetInfo(info *agent.Information)
	SetAuthReq(req *agent.AuthenticationRequest)
	SetAuthResp(resp *agent.AuthenticationResponse)
	IConnection
	IVaildator
}

// IConnection is interface that using in socket communication
type IConnection interface {
	Read() (data []byte, err error)
	Write(data []byte) error
	Close() error
}

// AgentType is client type which will be VXAgent or Browser
type AgentType int32

// Enumerate agent types
const (
	VXAgent AgentType = 0
	Browser AgentType = 1
)

// Constants for ping sender functionality
const (
	delayPingSender int    = 5000
	constPingPacket string = "PING"
)

var agentTypeName = map[int32]string{
	0: "VXAgent",
	1: "Browser",
}

var agentTypeValue = map[string]int32{
	"VXAgent": 0,
	"Browser": 1,
}

func (at AgentType) String() string {
	if str, ok := agentTypeName[int32(at)]; ok {
		return str
	}

	return "unknown"
}

// MarshalJSON using for convert from AgentType to JSON
func (at *AgentType) MarshalJSON() ([]byte, error) {
	if str, ok := agentTypeName[int32(*at)]; ok {
		return []byte(`"` + str + `"`), nil
	}

	return nil, errors.New("can't marshal AgentType")
}

// UnmarshalJSON using for convert from JSON to AgentType
func (at *AgentType) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)

	if name, ok := agentTypeValue[str]; ok {
		*at = AgentType(name)
		return nil
	}

	return errors.New("can't unmarshal AgentType")
}

// AuthenticationData is struct which contains information about authentication
type AuthenticationData struct {
	req  *agent.AuthenticationRequest
	resp *agent.AuthenticationResponse
}

// AgentInfo is struct which contains only public information about agent
type AgentInfo struct {
	ID   string             `json:"id"`
	IP   string             `json:"ip"`
	Src  string             `json:"src"`
	Dst  string             `json:"dst"`
	Type AgentType          `json:"type"`
	Info *agent.Information `json:"-"`
}

// agentSocket is struct that used for registration of connection data
type agentSocket struct {
	id    string
	ip    string
	src   string
	at    AgentType
	info  *agent.Information
	auth  *AuthenticationData
	mRecv *sync.Mutex
	mSend *sync.Mutex
	IConnection
	IVaildator
	IProtoIO
}

// GetPublicInfo is function which provided only public information about agent
func (as *agentSocket) GetPublicInfo() *AgentInfo {
	return &AgentInfo{
		ID:   as.id,
		IP:   as.ip,
		Src:  as.GetSource(),
		Dst:  as.GetDestination(),
		Type: as.at,
		Info: as.info,
	}
}

// GetSource is function which return source token
func (as *agentSocket) GetSource() string {
	return as.src
}

// SetSource is function which storing source token
func (as *agentSocket) SetSource(src string) {
	as.src = src
}

// SetInfo is function which storing information about agent
func (as *agentSocket) SetInfo(info *agent.Information) {
	as.info = info
}

// SetAuthReq is function which storing authentication request about agent after handshake
func (as *agentSocket) SetAuthReq(req *agent.AuthenticationRequest) {
	as.auth.req = req
}

// SetAuthResp is function which storing authentication response about agent after handshake
func (as *agentSocket) SetAuthResp(resp *agent.AuthenticationResponse) {
	as.auth.resp = resp
}

// GetDestination is function which return destination token
func (as *agentSocket) GetDestination() string {
	if as.auth != nil && as.auth.resp != nil {
		// This code used on agent side
		if as.auth.resp.GetAtoken() == as.src {
			return as.auth.resp.GetStoken()
		}
		// This code used on server side
		if as.auth.resp.GetStoken() == as.src {
			return as.auth.resp.GetAtoken()
		}
	}

	return ""
}

// runPingSender is function which periodic send ping packet to other side
func (as *agentSocket) runPingSender() func() {
	var closed bool
	var wg sync.WaitGroup
	closeChan := make(chan struct{})
	closeFunc := func() {
		if !closed {
			closed = true
			closeChan <- struct{}{}
			wg.Wait()
		}
	}
	handle := func() {
		as.mSend.Lock()
		defer as.mSend.Unlock()

		as.Write([]byte(constPingPacket))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-time.NewTimer(time.Millisecond * time.Duration(delayPingSender)).C:
				handle()
				continue
			case <-closeChan:
				break
			}

			break
		}
	}()

	return closeFunc
}

// recvPacket is function for serving packet to vxproto (hub) logic through callback
// Result is the success of packet processing otherwise will raise error
func (as *agentSocket) recvPacket() error {
	as.mRecv.Lock()
	defer as.mRecv.Unlock()

	packetData, err := as.Read()
	if err != nil {
		return err
	}

	// Using a ping functionality for supporting opened connection
	if bytes.Equal(packetData, []byte(constPingPacket)) {
		return nil
	}
	if bytes.HasPrefix(packetData, []byte(constPingPacket)) {
		packetData = packetData[4:]
	}
	if bytes.HasSuffix(packetData, []byte(constPingPacket)) {
		packetData = packetData[:len(packetData)-4]
	}

	packet, err := (&Packet{}).fromBytesPB(packetData)
	if err != nil {
		return err
	}

	err = as.IProtoIO.recvPacket(packet)
	if err != nil {
		return err
	}

	return nil
}

// sendPacket is function for sending packet to other side
// Result is the success of packet sending otherwise will raise error
func (as *agentSocket) sendPacket(packet *Packet) error {
	as.mSend.Lock()
	defer as.mSend.Unlock()

	packetData, err := packet.toBytesPB()
	if err != nil {
		return err
	}

	if err := as.Write(packetData); err != nil {
		return errors.New("failed sending of packet: " + err.Error())
	}

	return nil
}
