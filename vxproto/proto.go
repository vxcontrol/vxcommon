package vxproto

import (
	"bytes"
	"crypto/cipher"
	"crypto/des"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"net"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/vxcontrol/vxcommon/agent"
)

// IVXProto is main interface for vxproto package
type IVXProto interface {
	NewModule(name, agentID string) IModuleSocket
	AddModule(socket IModuleSocket) bool
	DelModule(socket IModuleSocket) bool
	GetAgentList() map[string]*AgentInfo
	GetAgentsCount() int
	Close() error
	INetProto
}

// INetProto is network interface for client and server implementation
type INetProto interface {
	Connect(config map[string]string) error
	Listen(config map[string]string) error
}

// IRouter is router interface for shared modules communication
type IRouter interface {
	GetRoutes() map[string]string
	GetRoute(dst string) string
	AddRoute(dst, src string) error
	DelRoute(dst string) error
}

// IProtoIO is internal interface for callbacks which will use as IO in agent and module
type IProtoIO interface {
	recvPacket(packet *Packet) error
	sendPacket(packet *Packet) error
}

// IDefaultReceiver is interface for receiver in main module for control data
type IDefaultReceiver interface {
	DefaultRecvPacket(packet *Packet) error
}

// IIMC is interface for inter modules communication
type IIMC interface {
	HasIMCTokenFormat(token string) bool
	GetIMCModuleSocket(token string) *moduleSocket
	MakeIMCToken(agentID, moduleName string) string
}

// IAgentValidator is interface for validator in main module for agent connection
type IAgentValidator interface {
	HasAgentIDValid(agentID string, agentType AgentType) bool
	HasAgentInfoValid(agentID string, info *agent.Information) bool
}

// IMainModule is interface for implementation common API server or agent
type IMainModule interface {
	IDefaultReceiver
	IAgentValidator
	OnConnect(socket IAgentSocket) error
}

// ITokenVaildator is interface for validate token information on handshake
type ITokenVaildator interface {
	HasTokenValid(token string) bool
	HasTokenCRCValid(token string) bool
	NewToken() string
}

// IVaildator is interface for validate agnet and token information on handshake
type IVaildator interface {
	IAgentValidator
	ITokenVaildator
}

const (
	// TokenTTLHours is time period for succeed token validation
	TokenTTLHours = 24
	// DelayHandleDeferredPacket is time period for sleep before handle deferred packet (ms)
	DelayHandleDeferredPacket = 100
	// MaxSizePacketQueue is maximum of size received or sent packets queue
	MaxSizePacketQueue = 5000
	// DeferredPacketTTLSeconds is time to live of deferred packet in seconds
	DeferredPacketTTLSeconds = 60
)

// vxProto struct is main object which will be use for Client and Server logics
type vxProto struct {
	modules  map[string]map[string]*moduleSocket
	agents   map[string]*agentSocket
	routes   map[string]string
	mutex    *sync.RWMutex
	tokenKey []byte
	closers  []func()
	rpqueue  []*Packet
	spqueue  []*Packet
	mxqueue  *sync.Mutex
	wgqueue  sync.WaitGroup
	IMainModule
}

// New is function which constructed vxProto object
func New(mmodule IMainModule) IVXProto {
	tokenKey := make([]byte, 24)
	if _, err := rand.Read(tokenKey); err != nil {
		return nil
	}

	return &vxProto{
		IMainModule: mmodule,
		modules:     make(map[string]map[string]*moduleSocket),
		agents:      make(map[string]*agentSocket),
		routes:      make(map[string]string),
		mutex:       &sync.RWMutex{},
		mxqueue:     &sync.Mutex{},
		tokenKey:    tokenKey,
	}
}

func (vxp *vxProto) runPQueue(handle func()) func() {
	vxp.mutex.Lock()
	var closed bool
	closeChan := make(chan struct{})
	closeFunc := func() {
		if !closed {
			closed = true
			closeChan <- struct{}{}
		}
	}
	vxp.closers = append(vxp.closers, closeFunc)
	vxp.mutex.Unlock()

	vxp.wgqueue.Add(1)
	go func() {
		defer vxp.wgqueue.Done()

		for {
			select {
			case <-time.NewTimer(time.Millisecond * time.Duration(DelayHandleDeferredPacket)).C:
				handle()
				continue
			case <-closeChan:
				vxp.mutex.Lock()
				for idx, closer := range vxp.closers {
					if reflect.DeepEqual(closer, closeFunc) {
						// It's possible because there used break in the bottom
						vxp.closers = append(vxp.closers[:idx], vxp.closers[idx+1:]...)
						break
					}
				}
				vxp.mutex.Unlock()
				break
			}

			break
		}
	}()

	return closeFunc
}

func (vxp *vxProto) runRecvPQueue() func() {
	return vxp.runPQueue(func() {
		vxp.mxqueue.Lock()
		pq := vxp.rpqueue
		vxp.rpqueue = vxp.rpqueue[:0]
		vxp.mxqueue.Unlock()
		for _, p := range pq {
			if time.Since(time.Unix(p.TS, 0)).Seconds() < DeferredPacketTTLSeconds {
				// Here using public method because there need save proc time to recv other packets
				vxp.recvPacket(p)
			}
		}
	})
}

func (vxp *vxProto) runSendPQueue() func() {
	return vxp.runPQueue(func() {
		vxp.mxqueue.Lock()
		pq := vxp.spqueue
		vxp.spqueue = vxp.spqueue[:0]
		vxp.mxqueue.Unlock()
		for _, p := range pq {
			if time.Since(time.Unix(p.TS, 0)).Seconds() < DeferredPacketTTLSeconds {
				// Here using public method because there need save proc time to send other packets
				vxp.sendPacket(p)
			}
		}
	})
}

// Close is function which will stop all agents and server
func (vxp *vxProto) Close() error {
	vxp.mutex.Lock()

	// Close all registred sockets
	for _, close := range vxp.closers {
		close()
	}

	// Remove and stop all registred agents
	for _, socket := range vxp.agents {
		if err := socket.Close(); err != nil {
			return err
		}
		// TODO: maybe it's not necessary because
		// Agent will be deleted in server or client logic
		if !vxp.delAgentI(socket) {
			return errors.New("failed deleting agent")
		}
	}

	vxp.mutex.Unlock()

	// TODO: here need delete all added Module Sockets
	vxp.wgqueue.Wait()

	return nil
}

// Connect is function for implement client network interface for many wrappers
// config argument should be include next fields:
// connection - described base URL to connect to server ("ws://localhost:8080")
//    in the connection field are supported next schemes: ws, wss
// id - means Agent ID for authentication on the server side
// token - means Agent Token for authorization each connection on the server side
func (vxp *vxProto) Connect(config map[string]string) error {
	// This function using public methods because here don't use mutex
	if _, ok := config["connection"]; !ok {
		return errors.New("field connection not found")
	}
	if _, ok := config["id"]; !ok {
		return errors.New("field id not found")
	}
	if _, ok := config["token"]; !ok {
		return errors.New("field token not found")
	}

	if err := vxp.parseURL(config, config["connection"]); err != nil {
		return err
	}

	recvCloseHandle := vxp.runRecvPQueue()
	defer recvCloseHandle()
	sendCloseHandle := vxp.runSendPQueue()
	defer sendCloseHandle()

	switch config["scheme"] {
	case "ws":
		fallthrough
	case "wss":
		return vxp.connectWS(config)
	default:
		return errors.New("unsupported protocol scheme")
	}
}

// Listen is function for implement server network interface for many wrappers
// config argument should be include next fields:
// listen - described base URL to listen on the server ("ws://0.0.0.0:8080")
//    in the listen field are supported next schemes: ws, wss
// ssl_cert - contains SSL certificate in PEM format (for wss URL scheme)
// ssl_key - contains SSL private key in PEM format (for wss URL scheme)
func (vxp *vxProto) Listen(config map[string]string) error {
	// This function using public methods because here don't use mutex
	if _, ok := config["listen"]; !ok {
		return errors.New("field listen not found")
	}

	if err := vxp.parseURL(config, config["listen"]); err != nil {
		return err
	}

	recvCloseHandle := vxp.runRecvPQueue()
	defer recvCloseHandle()
	sendCloseHandle := vxp.runSendPQueue()
	defer sendCloseHandle()

	switch config["scheme"] {
	case "ws":
		return vxp.listenWS(config)
	case "wss":
		if _, ok := config["ssl_cert"]; !ok {
			return errors.New("field ssl_cert not found")
		}
		if _, ok := config["ssl_key"]; !ok {
			return errors.New("field ssl_key not found")
		}
		// TODO: here need validate SSL pair of cert and key
		return vxp.listenWS(config)
	default:
		return errors.New("unsupported protocol scheme")
	}
}

// NewModule is function which used for new module creation
func (vxp *vxProto) NewModule(name, agentID string) IModuleSocket {
	return &moduleSocket{
		name:             name,
		agentID:          agentID,
		imcToken:         vxp.MakeIMCToken(agentID, name),
		router:           newRouter(),
		IIMC:             vxp,
		IRouter:          vxp,
		IProtoIO:         vxp,
		IDefaultReceiver: vxp,
	}
}

// AddModule is function which used for new module registration
func (vxp *vxProto) AddModule(socket IModuleSocket) bool {
	vxp.mutex.Lock()
	defer vxp.mutex.Unlock()

	mSocket := socket.(*moduleSocket)
	if module, ok := vxp.modules[mSocket.name]; !ok {
		vxp.modules[mSocket.name] = make(map[string]*moduleSocket)
		vxp.modules[mSocket.name][mSocket.agentID] = mSocket
	} else if _, ok := module[mSocket.agentID]; !ok {
		vxp.modules[mSocket.name][mSocket.agentID] = mSocket
	} else {
		return false
	}

	return true
}

// DelModule is function which used for delete module object
func (vxp *vxProto) DelModule(socket IModuleSocket) bool {
	vxp.mutex.Lock()
	defer vxp.mutex.Unlock()

	mSocket := socket.(*moduleSocket)
	if module, ok := vxp.modules[mSocket.name]; ok {
		if _, ok := module[mSocket.agentID]; ok {
			mSocket.Close()
			delete(vxp.modules[mSocket.name], mSocket.agentID)
			if len(vxp.modules[mSocket.name]) == 0 {
				delete(vxp.modules, mSocket.name)
			}
			return true
		}
	}

	return false
}

// GetRoutes is function for get routes to existing destination points
func (vxp *vxProto) GetRoutes() map[string]string {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	return vxp.routes
}

// GetRoutes is function for get route to this destination points
func (vxp *vxProto) GetRoute(dst string) string {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	if src, ok := vxp.routes[dst]; ok {
		return src
	}

	return ""
}

// AddRoute is function for add route to new destination point by source token
func (vxp *vxProto) AddRoute(dst, src string) error {
	vxp.mutex.Lock()
	defer vxp.mutex.Unlock()

	if _, ok := vxp.routes[dst]; ok {
		return errors.New("route to this destination already exist")
	}

	if vxp.getAgentBySrcI(src) == nil {
		return errors.New("source token doesn't exist in the proto")
	}

	vxp.routes[dst] = src

	return nil
}

// DelRoute is function for delete route to exists destination point
func (vxp *vxProto) DelRoute(dst string) error {
	vxp.mutex.Lock()
	defer vxp.mutex.Unlock()

	if _, ok := vxp.routes[dst]; !ok {
		return errors.New("route to this destination doesen't exist")
	}

	delete(vxp.routes, dst)

	return nil
}

// GetAgentList is function which return agents list with info structure
func (vxp *vxProto) GetAgentList() map[string]*AgentInfo {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	agents := make(map[string]*AgentInfo)
	for _, agent := range vxp.agents {
		agents[agent.GetDestination()] = agent.GetPublicInfo()
	}

	return agents
}

// GetAgentList is function which return agents amount
func (vxp *vxProto) GetAgentsCount() int {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	return len(vxp.agents)
}

// getCRC32 is function for making CRC32 bytes from data bytes
func (vxp *vxProto) getCRC32(data []byte) []byte {
	crc32q := crc32.MakeTable(0xD5828281)
	crc32t := crc32.Checksum(data, crc32q)
	crc32r := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc32r, uint32(crc32t))

	return crc32r
}

// HasTokenValid is function which validate agent token
func (vxp *vxProto) HasTokenValid(token string) bool {
	if len(token) != 40 {
		return false
	}

	tokenBytes, err := hex.DecodeString(token)
	if err != nil {
		return false
	}

	secret := tokenBytes[:16]
	if !bytes.Equal(tokenBytes[16:], vxp.getCRC32(secret)) {
		return false
	}

	block, err := des.NewTripleDESCipher(vxp.tokenKey)
	if err != nil {
		return false
	}
	iv := vxp.tokenKey[:des.BlockSize]
	mode := cipher.NewCBCDecrypter(block, iv)
	plainPayload := make([]byte, len(secret))
	mode.CryptBlocks(plainPayload, secret)

	tokenPayload := plainPayload[:12]
	crc32Payload := plainPayload[12:]
	if !bytes.Equal(crc32Payload, vxp.getCRC32(tokenPayload)) {
		return false
	}

	ts := time.Unix(int64(binary.LittleEndian.Uint64(tokenPayload[4:])), 0)
	duration := time.Since(ts)
	if duration.Hours() > TokenTTLHours {
		return false
	}

	return true
}

// HasTokenCRCValid is function which validate CRC for agent token
func (vxp *vxProto) HasTokenCRCValid(token string) bool {
	if len(token) != 40 {
		return false
	}

	tokenBytes, err := hex.DecodeString(token)
	if err != nil {
		return false
	}

	if !bytes.Equal(tokenBytes[16:], vxp.getCRC32(tokenBytes[:16])) {
		return false
	}

	return true
}

// NewToken is function which generate new token
// Rand: 4 random bytes which generated on server side
// TS: 8 bytes timestamp of generation time
// Payload: 12 bytes (Rand + TS)
// IV: initialization vector for 3DES cipher is first bytes from key
// Secret: 3DES(Payload + CRC32(Payload))
// Token: Secret + CRC32(Secret)
func (vxp *vxProto) NewToken() string {
	tokenRand := make([]byte, 4)
	if _, err := rand.Read(tokenRand); err != nil {
		return ""
	}

	ts := make([]byte, 8)
	binary.LittleEndian.PutUint64(ts, uint64(time.Now().Unix()))

	tokenPayload := append(tokenRand[:], ts[:]...)
	crc32Payload := vxp.getCRC32(tokenPayload)

	block, err := des.NewTripleDESCipher(vxp.tokenKey)
	if err != nil {
		return ""
	}
	iv := vxp.tokenKey[:des.BlockSize]
	mode := cipher.NewCBCEncrypter(block, iv)

	plainPayload := append(tokenPayload[:], crc32Payload[:]...)
	secret := make([]byte, len(plainPayload))
	mode.CryptBlocks(secret, plainPayload)
	tokenBytes := append(secret[:], vxp.getCRC32(secret)...)
	token := hex.EncodeToString(tokenBytes)

	return token
}

// HasIMCTokenFormat is function which validate imc token format
func (vxp *vxProto) HasIMCTokenFormat(token string) bool {
	if len(token) != 40 || !strings.HasPrefix(token, "ffffffff") {
		return false
	}

	return true
}

// GetIMCModuleSocket is function which get module socket for imc token
// The function may return nil if the token doesn't exist
func (vxp *vxProto) GetIMCModuleSocket(token string) *moduleSocket {
	vxp.mutex.Lock()
	defer vxp.mutex.Unlock()

	for _, module := range vxp.modules {
		for _, ms := range module {
			if ms.imcToken == token {
				return ms
			}
		}
	}

	return nil
}

// MakeIMCToken is function which get new imc token
func (vxp *vxProto) MakeIMCToken(agentID, moduleName string) string {
	hash := md5.Sum(append([]byte(agentID+":"+moduleName+":"), vxp.tokenKey...))
	return "ffffffff" + hex.EncodeToString(hash[:])
}

// parseURL is function which return config with parsed URL
func (vxp *vxProto) parseURL(config map[string]string, sURL string) error {
	stURL, err := url.Parse(sURL)
	if err != nil {
		return err
	}

	config["scheme"] = stURL.Scheme
	config["socket"] = stURL.Host

	host, port, err := net.SplitHostPort(stURL.Host)
	if err != nil {
		config["host"] = stURL.Host
		switch stURL.Scheme {
		case "ws":
			config["port"] = "80"
		case "wss":
			config["port"] = "443"
		default:
			config["port"] = "8080"
		}
	} else {
		config["host"] = host
		config["port"] = port
	}
	config["url"] = stURL.Scheme + "://" + stURL.Host

	return nil
}

// getModule is function which used for get module object
func (vxp *vxProto) getModule(name, agentID string) *moduleSocket {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	return vxp.getModuleI(name, agentID)
}

// getModuleI is internal function which used for get module object
func (vxp *vxProto) getModuleI(name, agentID string) *moduleSocket {
	if module, ok := vxp.modules[name]; ok {
		if ms, ok := module[agentID]; ok {
			return ms
		}
		// This case for shared modules
		if ms, ok := module[""]; ok {
			return ms
		}
	}

	return nil
}

// addAgent is function which used for new agent registration
func (vxp *vxProto) addAgent(socket *agentSocket) bool {
	vxp.mutex.Lock()
	defer vxp.mutex.Unlock()

	return vxp.addAgentI(socket)
}

// addAgentI is internal function which used for new agent registration
func (vxp *vxProto) addAgentI(socket *agentSocket) bool {
	if vxp.getAgentBySrcI(socket.src) != nil {
		return false
	}

	vxp.agents[socket.src] = socket
	vxp.routes[socket.GetDestination()] = socket.src
	// Notify all modules that the agent has been connected
	// TODO: maybe here need add src and dst information
	packet := &Packet{
		PType: PTControl,
		Payload: &ControlMessage{
			AgentInfo: socket.GetPublicInfo(),
			MsgType:   AgentConnected,
		},
	}
	for _, module := range vxp.modules {
		for _, ms := range module {
			ms.GetReceiver() <- packet
		}
	}

	return true
}

// delAgent is function which used for delete agent object
func (vxp *vxProto) delAgent(socket *agentSocket) bool {
	vxp.mutex.Lock()
	defer vxp.mutex.Unlock()

	return vxp.delAgentI(socket)
}

// delAgentI is internal function which used for delete agent object
func (vxp *vxProto) delAgentI(socket *agentSocket) bool {
	if vxp.getAgentBySrcI(socket.src) != nil {
		var dsts []string
		for dst, src := range vxp.routes {
			if src == socket.src {
				dsts = append(dsts, dst)
			}
		}
		for _, dst := range dsts {
			delete(vxp.routes, dst)
		}
		// Notify all modules that the agent has been disconnected
		// TODO: maybe here need add src and dst information
		packet := &Packet{
			PType: PTControl,
			Payload: &ControlMessage{
				AgentInfo: socket.GetPublicInfo(),
				MsgType:   AgentDisconnected,
			},
		}
		for _, module := range vxp.modules {
			for _, ms := range module {
				ms.router.unlock(socket.src)
				ms.GetReceiver() <- packet
			}
		}
		delete(vxp.agents, socket.src)
		return true
	}

	return false
}

// getAgentBySrc is function which used for get agent object by source
func (vxp *vxProto) getAgentBySrc(src string) *agentSocket {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	return vxp.getAgentBySrcI(src)
}

// getAgentBySrcI is internal function which used for get agent object
func (vxp *vxProto) getAgentBySrcI(src string) *agentSocket {
	if agent, ok := vxp.agents[src]; ok {
		return agent
	}

	return nil
}

// getAgentByDst is function which used for get agent object by destination
func (vxp *vxProto) getAgentByDst(dst string) *agentSocket {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	return vxp.getAgentByDstI(dst)
}

// getAgentByDstI is internal function which used for get agent object
func (vxp *vxProto) getAgentByDstI(dst string) *agentSocket {
	for rdst, rsrc := range vxp.routes {
		if agent, ok := vxp.agents[rsrc]; rdst == dst && ok {
			return agent
		}
	}

	return nil
}

// recvPacket is function for serving packet to target module
// Result is the success of packet processing otherwise will raise error
func (vxp *vxProto) recvPacket(packet *Packet) error {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	return vxp.recvPacketI(packet)
}

// recvPacketI is internal function for serving packet to target module
// Result is the success of packet processing otherwise will raise error
func (vxp *vxProto) recvPacketI(packet *Packet) error {
	// Case for direct forward packets
	if _, ok := vxp.routes[packet.Dst]; ok {
		return vxp.sendPacketI(packet)
	}

	hDeferredPacket := func(packet *Packet) error {
		vxp.mxqueue.Lock()
		defer vxp.mxqueue.Unlock()

		if len(vxp.rpqueue) >= MaxSizePacketQueue {
			vxp.rpqueue = vxp.rpqueue[1:]
		}
		vxp.rpqueue = append(vxp.rpqueue, packet)

		return nil
	}

	// Case for local accept packets
	as := vxp.getAgentBySrcI(packet.Dst)
	if as == nil {
		// Failed getting of agent information
		return hDeferredPacket(packet)
	}
	socket := vxp.getModuleI(packet.Module, as.id)
	if socket == nil {
		// Failed getting of module
		return hDeferredPacket(packet)
	}

	return socket.recvPacket(packet)
}

// sendPacket is function for sending packet to target agent
// Result is the success of packet sending otherwise will raise error
func (vxp *vxProto) sendPacket(packet *Packet) error {
	vxp.mutex.RLock()
	defer vxp.mutex.RUnlock()

	return vxp.sendPacketI(packet)
}

// sendPacket is internal function for sending packet to target agent
// Result is the success of packet sending otherwise will raise error
func (vxp *vxProto) sendPacketI(packet *Packet) error {
	hDeferredPacket := func(packet *Packet) error {
		vxp.mxqueue.Lock()
		defer vxp.mxqueue.Unlock()

		if len(vxp.spqueue) >= MaxSizePacketQueue {
			vxp.spqueue = vxp.spqueue[1:]
		}
		vxp.spqueue = append(vxp.spqueue, packet)

		return nil
	}

	socket := vxp.getAgentByDstI(packet.Dst)
	if socket == nil {
		// Failed getting of agent
		return hDeferredPacket(packet)
	}

	if packet.Src == "" {
		packet.Src = socket.src
	}

	return socket.sendPacket(packet)
}
