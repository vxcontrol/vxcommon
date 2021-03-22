package lua

import (
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/vxcontrol/luar"
	"github.com/vxcontrol/vxcommon/agent"
	"github.com/vxcontrol/vxcommon/vxproto"
)

type recvCallbacks struct {
	recvData   *luaCallback
	recvText   *luaCallback
	recvFile   *luaCallback
	recvMsg    *luaCallback
	controlMsg *luaCallback
}

// Module is struct that used for internal communication logic with Lua state
type Module struct {
	state    *State
	logger   *logrus.Entry
	socket   vxproto.IModuleSocket
	result   string
	cbs      recvCallbacks
	waitTime int64
	wgRun    sync.WaitGroup
	agents   map[string]*vxproto.AgentInfo
	args     map[string][]string
	quit     chan struct{}
	closed   bool
}

// IsClose is nonblocked function which check a state of module
func (m *Module) IsClose() bool {
	return m.closed || m.state == nil
}

// GetResult is nonblocked function which return result from module
func (m *Module) GetResult() string {
	return m.result
}

// Start is function which prepare state for module
func (m *Module) Start() {
	if m.state == nil {
		return
	}
	if m.closed && m.state.closed {
		m.quit = make(chan struct{})
		m.closed = false
	} else {
		return
	}

	m.wgRun.Add(1)
	go func() {
		defer m.wgRun.Done()
		m.recvPacket()
	}()
	m.logger.Info("the module was started")
	defer m.logger.Info("the module was stopped")

	var err error
	m.result = ""
	m.wgRun.Add(1)
	defer m.wgRun.Done()
	for m.result, err = m.state.Exec(); err != nil && !m.closed; {
		// TODO: here need using store message about problem into DB
		msg := "error executing the module code on the lua state"
		m.logger.WithError(err).WithField("result", m.result).Error(msg)
		time.Sleep(time.Second * time.Duration(5))
		m.state.L.SetTop(0)
	}
}

// Stop is function which set close state for module
func (m *Module) Stop() {
	if m.closed || m.state == nil {
		return
	}

	m.logger.Info("the module wants to stop")
	defer m.logger.Info("the module stopping has done")

	m.closed = true
	m.controlMsgCb("quit", "")
	close(m.quit)

	m.wgRun.Wait()
	m.delCbs([]interface{}{"data", "text", "file", "msg", "control"})
}

// Close is function which release lua state for module
func (m *Module) Close() {
	if m.state == nil {
		return
	}

	m.Stop()
	luar.Register(m.state.L, "__api", luar.Map{})
	luar.Register(m.state.L, "__agents", luar.Map{})
	luar.Register(m.state.L, "__routes", luar.Map{})
	luar.Register(m.state.L, "__imc", luar.Map{})
	m.state = nil
}

// SetAgents is function for storing agent list into module state
func (m *Module) SetAgents(agents map[string]*vxproto.AgentInfo) {
	m.agents = agents
}

// ControlMsg is function for send control message to module state
func (m *Module) ControlMsg(mtype, data string) bool {
	if m.closed || m.state == nil {
		return false
	}

	return m.controlMsgCb(mtype, data)
}

// Set timeout for all blocked functions
// If used timeout variable in non-zero value, it will wake up after timeout
// Timeout variable uses in milliseconds and -1 value means infinity
func (m *Module) setRecvTimeout(timeout int64) {
	m.waitTime = timeout
}

// await is blocked function which wait a close of module
// If used timeout variable in non-zero value, it will wake up after timeout
// Timeout variable uses in milliseconds and -1 value means infinity
func (m *Module) await(timeout int64) {
	if m.closed {
		return
	}

	m.state.L.Unlock()
	defer m.state.L.Lock()
	runtime.Gosched()

	if timeout >= 0 {
		select {
		case <-time.NewTimer(time.Millisecond * time.Duration(timeout)).C:
		case <-m.quit:
		}
	} else if timeout < 0 {
		<-m.quit
	}
}

func (m *Module) getName() string {
	return m.socket.GetName()
}

func (m *Module) getOS() string {
	return runtime.GOOS
}

func (m *Module) getArch() string {
	return runtime.GOARCH
}

func (m *Module) getAgents() map[string]vxproto.AgentInfo {
	agents := make(map[string]vxproto.AgentInfo, 0)
	for t, a := range m.agents {
		ca := *a
		if a.Info != nil {
			ca.Info = proto.Clone(a.Info).(*agent.Information)
			if a.Info.Os != nil {
				ca.Info.Os = proto.Clone(a.Info.Os).(*agent.Information_OS)
			}
			if a.Info.User != nil {
				ca.Info.User = proto.Clone(a.Info.User).(*agent.Information_User)
			}
		}
		agents[t] = ca
	}
	return agents
}

func (m *Module) getAgentsCount() int {
	return len(m.agents)
}

func (m *Module) getAgentsByID(agentID string) map[string]vxproto.AgentInfo {
	agents := m.getAgents()
	filtered := make(map[string]vxproto.AgentInfo, 0)
	for src, info := range agents {
		if info.ID == agentID {
			filtered[src] = info
		}
	}
	return filtered
}

func (m *Module) getAgentsBySrc(srcToken string) map[string]vxproto.AgentInfo {
	agents := m.getAgents()
	filtered := make(map[string]vxproto.AgentInfo, 0)
	for src, info := range agents {
		if info.Src == srcToken {
			filtered[src] = info
		}
	}
	return filtered
}

func (m *Module) getAgentsByDst(dstToken string) map[string]vxproto.AgentInfo {
	agents := m.getAgents()
	filtered := make(map[string]vxproto.AgentInfo, 0)
	for src, info := range agents {
		if info.Dst == dstToken {
			filtered[src] = info
		}
	}
	return filtered
}

func (m *Module) getIMCToken() string {
	return m.socket.GetIMCToken()
}

func (m *Module) getIMCTokenInfo(token string) (string, string, bool) {
	ms := m.socket.GetIMCModuleSocket(token)
	if ms == nil {
		return "", "", false
	}
	return ms.GetAgentID(), ms.GetName(), true
}

func (m *Module) isIMCTokenExist(token string) bool {
	ms := m.socket.GetIMCModuleSocket(token)
	if ms == nil {
		return false
	}
	return true
}

func (m *Module) makeIMCToken(agentID, moduleName string) string {
	return m.socket.MakeIMCToken(agentID, moduleName)
}

func (m *Module) getRoutes() map[string]string {
	return m.socket.GetRoutes()
}

func (m *Module) getRoutesCount() int {
	return len(m.socket.GetRoutes())
}

func (m *Module) getRoute(dst string) string {
	return m.socket.GetRoute(dst)
}

func (m *Module) addRoute(dst, src string) bool {
	m.logger.WithFields(logrus.Fields{
		"src": src,
		"dst": dst,
	}).Debug("the module added the new route")
	return m.socket.AddRoute(dst, src) == nil
}

func (m *Module) delRoute(dst string) bool {
	m.logger.WithFields(logrus.Fields{
		"dst": dst,
	}).Debug("the module deleted the route")
	return m.socket.DelRoute(dst) == nil
}

func (m *Module) sendDataTo(dst, data string) bool {
	if len(data) == 0 {
		return false
	}

	sdata := &vxproto.Data{
		Data: []byte(data),
	}

	m.logger.WithFields(logrus.Fields{
		"len": len(data),
		"dst": dst,
	}).Debug("the module sent data")
	return m.socket.SendDataTo(dst, sdata) == nil
}

func (m *Module) sendFileTo(dst, data, name string) bool {
	if len(data) == 0 || name == "" {
		return false
	}

	sfile := &vxproto.File{
		Data: []byte(data),
		Name: name,
	}

	m.logger.WithFields(logrus.Fields{
		"len":  len(data),
		"name": name,
		"dst":  dst,
	}).Debug("the module sent file from data")
	return m.socket.SendFileTo(dst, sfile) == nil
}

func (m *Module) sendFileFromFSTo(dst, path, name string) bool {
	if path == "" || name == "" {
		return false
	}

	sfile := &vxproto.File{
		Path: name,
		Name: name,
	}

	m.logger.WithFields(logrus.Fields{
		"path": path,
		"name": name,
		"dst":  dst,
	}).Debug("the module sent file from fs")
	return m.socket.SendFileTo(dst, sfile) == nil
}

func (m *Module) sendTextTo(dst, data, name string) bool {
	if len(data) == 0 || name == "" {
		return false
	}

	stext := &vxproto.Text{
		Data: []byte(data),
		Name: name,
	}

	m.logger.WithFields(logrus.Fields{
		"len":  len(data),
		"name": name,
		"dst":  dst,
	}).Debug("the module sent text from data")
	return m.socket.SendTextTo(dst, stext) == nil
}

func (m *Module) sendMsgTo(dst, data string, mtype int32) bool {
	if len(data) == 0 || mtype < 0 || mtype > 3 {
		return false
	}

	msg := &vxproto.Msg{
		Data:  []byte(data),
		MType: vxproto.MsgType(mtype),
	}

	m.logger.WithFields(logrus.Fields{
		"len":  len(data),
		"type": vxproto.MsgType(mtype).String(),
		"dst":  dst,
	}).Debug("the module sent message")
	return m.socket.SendMsgTo(dst, msg) == nil
}

func (m *Module) recvDataCb(src string, data *vxproto.Data) bool {
	if m.cbs.recvData != nil {
		res := new(bool)
		m.cbs.recvData.Call(&res, src, string(data.Data[:]))
		return *res
	}

	return false
}

func (m *Module) recvFileCb(src string, file *vxproto.File) bool {
	if m.cbs.recvFile != nil {
		res := new(bool)
		m.cbs.recvFile.Call(&res, src, file.Path, file.Name)
		return *res
	}

	return false
}

func (m *Module) recvTextCb(src string, text *vxproto.Text) bool {
	if m.cbs.recvText != nil {
		res := new(bool)
		m.cbs.recvText.Call(&res, src, string(text.Data[:]), text.Name)
		return *res
	}

	return false
}

func (m *Module) recvMsgCb(src string, msg *vxproto.Msg) bool {
	if m.cbs.recvMsg != nil {
		res := new(bool)
		m.cbs.recvMsg.Call(&res, src, string(msg.Data[:]), int32(msg.MType))
		return *res
	}

	return false
}

func (m *Module) controlMsgCb(mtype, data string) bool {
	if m.cbs.controlMsg != nil {
		res := new(bool)
		m.cbs.controlMsg.Call(&res, mtype, data)
		return *res
	}

	return false
}

func (m *Module) recvData() (string, string, bool) {
	src, data, err := m.socket.RecvData(m.waitTime)
	if err != nil {
		return "", "", false
	}

	return src, string(data.Data[:]), true
}

func (m *Module) recvFile() (string, string, string, bool) {
	src, file, err := m.socket.RecvFile(m.waitTime)
	if err != nil {
		return "", "", "", false
	}

	return src, file.Path, file.Name, true
}

func (m *Module) recvText() (string, string, string, bool) {
	src, text, err := m.socket.RecvText(m.waitTime)
	if err != nil {
		return "", "", "", false
	}

	return src, string(text.Data[:]), text.Name, true
}

func (m *Module) recvMsg() (string, string, int32, bool) {
	src, msg, err := m.socket.RecvMsg(m.waitTime)
	if err != nil {
		return "", "", 0, false
	}

	return src, string(msg.Data[:]), int32(msg.MType), true
}

func (m *Module) recvDataFrom(src string) (string, bool) {
	data, err := m.socket.RecvDataFrom(src, m.waitTime)
	if err != nil {
		return "", false
	}

	return string(data.Data[:]), true
}

func (m *Module) recvFileFrom(src string) (string, string, bool) {
	file, err := m.socket.RecvFileFrom(src, m.waitTime)
	if err != nil {
		return "", "", false
	}

	return file.Path, file.Name, true
}

func (m *Module) recvTextFrom(src string) (string, string, bool) {
	text, err := m.socket.RecvTextFrom(src, m.waitTime)
	if err != nil {
		return "", "", false
	}

	return string(text.Data[:]), text.Name, true
}

func (m *Module) recvMsgFrom(src string) (string, int32, bool) {
	msg, err := m.socket.RecvMsgFrom(src, m.waitTime)
	if err != nil {
		return "", 0, false
	}

	return string(msg.Data[:]), int32(msg.MType), true
}

func (m *Module) addCbs(callbackTable interface{}) bool {
	callbackMap, ok := callbackTable.(map[string]interface{})
	if !ok {
		return false
	}

	for name, callback := range callbackMap {
		switch name {
		case "data":
			if cb, ok := callback.(*luar.LuaObject); ok {
				cb.Push()
				m.cbs.recvData = newLuaCallback(m.state.L)
				cb.Close()
				m.logger.Debug("the module added receive data callback")
			}
		case "text":
			if cb, ok := callback.(*luar.LuaObject); ok {
				cb.Push()
				m.cbs.recvText = newLuaCallback(m.state.L)
				cb.Close()
				m.logger.Debug("the module added receive text callback")
			}
		case "file":
			if cb, ok := callback.(*luar.LuaObject); ok {
				cb.Push()
				m.cbs.recvFile = newLuaCallback(m.state.L)
				cb.Close()
				m.logger.Debug("the module added receive file callback")
			}
		case "msg":
			if cb, ok := callback.(*luar.LuaObject); ok {
				cb.Push()
				m.cbs.recvMsg = newLuaCallback(m.state.L)
				cb.Close()
				m.logger.Debug("the module added receive message callback")
			}
		case "control":
			if cb, ok := callback.(*luar.LuaObject); ok {
				cb.Push()
				m.cbs.controlMsg = newLuaCallback(m.state.L)
				cb.Close()
				m.logger.Debug("the module added receive control message callback")
			}
		default:
		}
	}

	return true
}

func (m *Module) delCbs(CallbackTable interface{}) bool {
	callbackMap, ok := CallbackTable.([]interface{})
	if !ok {
		return false
	}

	for _, name := range callbackMap {
		switch name.(string) {
		case "data":
			if m.cbs.recvData != nil {
				m.cbs.recvData.Close()
				m.cbs.recvData = nil
				m.logger.Debug("the module deleted receive data callback")
			}
		case "text":
			if m.cbs.recvText != nil {
				m.cbs.recvText.Close()
				m.cbs.recvText = nil
				m.logger.Debug("the module deleted receive text callback")
			}
		case "file":
			if m.cbs.recvFile != nil {
				m.cbs.recvFile.Close()
				m.cbs.recvFile = nil
				m.logger.Debug("the module deleted receive file callback")
			}
		case "msg":
			if m.cbs.recvMsg != nil {
				m.cbs.recvMsg.Close()
				m.cbs.recvMsg = nil
				m.logger.Debug("the module deleted receive message callback")
			}
		case "control":
			if m.cbs.controlMsg != nil {
				m.cbs.controlMsg.Close()
				m.cbs.controlMsg = nil
				m.logger.Debug("the module deleted receive control message callback")
			}
		default:
		}
	}

	return true
}

func (m *Module) recvPacket() error {
	defer m.logger.Info("packet receiver was stopped")

	m.logger.Info("packet receiver was started")
	receiver := m.socket.GetReceiver()
	if receiver == nil {
		m.logger.Error("failed to initialize packet receiver")
		return errors.New("failed to initialize packet receiver")
	}
	for !m.closed {
		var packet *vxproto.Packet
		select {
		case packet = <-receiver:
		case <-m.quit:
			m.logger.Info("got signal to quit from channel")
			return nil
		}

		if packet == nil {
			m.logger.Error("failed receive packet")
			return errors.New("failed receive packet")
		}

		logger := m.logger.WithFields(logrus.Fields{
			"type": packet.PType.String(),
			"src":  packet.Src,
			"dst":  packet.Dst,
		})
		logger.Debug("packet receiver got new packet")
		switch packet.PType {
		case vxproto.PTData:
			m.recvDataCb(packet.Src, packet.GetData())
		case vxproto.PTFile:
			m.recvFileCb(packet.Src, packet.GetFile())
		case vxproto.PTText:
			m.recvTextCb(packet.Src, packet.GetText())
		case vxproto.PTMsg:
			m.recvMsgCb(packet.Src, packet.GetMsg())
		case vxproto.PTControl:
			msg := packet.GetControlMsg()
			switch msg.MsgType {
			case vxproto.AgentConnected:
				logger.Info("agent connected to the module")
				m.agents[msg.AgentInfo.Dst] = msg.AgentInfo
				m.controlMsgCb("agent_connected", msg.AgentInfo.Dst)
			case vxproto.AgentDisconnected:
				logger.Info("agent disconnected from the module")
				m.controlMsgCb("agent_disconnected", msg.AgentInfo.Dst)
				delete(m.agents, msg.AgentInfo.Dst)
			case vxproto.StopModule:
				logger.Info("got packet with signal to stop module")
				return nil
			}
		default:
			logger.Error("got packet has unexpected packet type")
			return errors.New("unexpected packet type")
		}
	}

	return nil
}

// NewModule is function which constructed Module object
func NewModule(args map[string][]string, state *State, socket vxproto.IModuleSocket) (*Module, error) {
	if socket == nil {
		logrus.Error("failed to make new module because socket object unset")
		return nil, errors.New("socket object not initialized")
	}

	m := &Module{
		socket: socket,
		state:  state,
		args:   args,
		agents: make(map[string]*vxproto.AgentInfo),
		closed: true,
		logger: logrus.WithFields(logrus.Fields{
			"component": "module",
			"module":    socket.GetName(),
			"agent":     socket.GetAgentID(),
		}),
	}

	luar.Register(state.L, "__api", luar.Map{
		// Functions
		"await":    m.await,
		"is_close": m.IsClose,
		"get_name": m.getName,
		"get_os":   m.getOS,
		"get_arch": m.getArch,
		"unsafe": luar.Map{
			"lock":   func() { m.state.L.Lock() },
			"unlock": func() { m.state.L.Unlock() },
		},

		"add_cbs":          m.addCbs,
		"del_cbs":          m.delCbs,
		"set_recv_timeout": m.setRecvTimeout,

		"send_data_to":         m.sendDataTo,
		"send_file_to":         m.sendFileTo,
		"send_text_to":         m.sendTextTo,
		"send_msg_to":          m.sendMsgTo,
		"send_file_from_fs_to": m.sendFileFromFSTo,

		"recv_data": m.recvData,
		"recv_file": m.recvFile,
		"recv_text": m.recvText,
		"recv_msg":  m.recvMsg,

		"recv_data_from": m.recvDataFrom,
		"recv_file_from": m.recvFileFrom,
		"recv_text_from": m.recvTextFrom,
		"recv_msg_from":  m.recvMsgFrom,
	})

	luar.Register(state.L, "__agents", luar.Map{
		// Functions
		"dump":       m.getAgents,
		"count":      m.getAgentsCount,
		"get_by_id":  m.getAgentsByID,
		"get_by_src": m.getAgentsBySrc,
		"get_by_dst": m.getAgentsByDst,
	})

	luar.Register(state.L, "__routes", luar.Map{
		// Functions
		"dump":  m.getRoutes,
		"count": m.getRoutesCount,
		"get":   m.getRoute,
		"add":   m.addRoute,
		"del":   m.delRoute,
	})

	luar.Register(state.L, "__imc", luar.Map{
		// Functions
		"get_token":  m.getIMCToken,
		"get_info":   m.getIMCTokenInfo,
		"is_exist":   m.isIMCTokenExist,
		"make_token": m.makeIMCToken,
	})

	luar.GoToLua(state.L, args)
	state.L.SetGlobal("__args")

	// TODO: change it to native load function
	state.L.DoString(`
	io.stdout:setvbuf('no')

	function __api.async(f, ...)
		local glue = require("glue")
		__api.unsafe.unlock()
		t = glue.pack(f(...))
		__api.unsafe.lock()
		return glue.unpack(t)
	end

	function __api.sync(f, ...)
		local glue = require("glue")
		__api.unsafe.lock()
		t = glue.pack(f(...))
		__api.unsafe.unlock()
		return glue.unpack(t)
	end
	`)

	m.logger.Info("the module was created")
	return m, nil
}
