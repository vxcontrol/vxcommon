package vxproto

import (
	"errors"
	"time"
)

// IModuleSocket is main interface for Module Socket integration
type IModuleSocket interface {
	GetName() string
	GetAgentID() string
	GetIMCToken() string
	GetReceiver() chan *Packet
	RecvData(timeout int64) (string, *Data, error)
	RecvFile(timeout int64) (string, *File, error)
	RecvText(timeout int64) (string, *Text, error)
	RecvMsg(timeout int64) (string, *Msg, error)
	RecvDataFrom(src string, timeout int64) (*Data, error)
	RecvFileFrom(src string, timeout int64) (*File, error)
	RecvTextFrom(src string, timeout int64) (*Text, error)
	RecvMsgFrom(src string, timeout int64) (*Msg, error)
	SendDataTo(dst string, data *Data) error
	SendFileTo(dst string, file *File) error
	SendTextTo(dst string, text *Text) error
	SendMsgTo(dst string, msg *Msg) error
	Close()
	IRouter
	IIMC
}

// moduleSocket is struct that used for receive data from other side to ms
type moduleSocket struct {
	name     string
	agentID  string
	imcToken string
	router   *recvRouter
	IDefaultReceiver
	IProtoIO
	IRouter
	IIMC
}

// Close is function which stop all blockers and valid close this socket
func (ms *moduleSocket) Close() {
	if ms.router != nil {
		ms.router.unlockAll()
	}
}

// GetName is function which using for access to private property name
func (ms *moduleSocket) GetName() string {
	return ms.name
}

// GetName is function which using for access to private property agentID
func (ms *moduleSocket) GetAgentID() string {
	return ms.agentID
}

// GetIMCToken is function which using for access to private property imcToken
func (ms *moduleSocket) GetIMCToken() string {
	return ms.imcToken
}

// GetChannels is function which exported channels for receive packets and control
func (ms *moduleSocket) GetReceiver() chan *Packet {
	if ms.router == nil {
		return nil
	}
	return ms.router.receiver
}

// RecvData is function for receive Data packet with timeout (ms)
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvData(timeout int64) (string, *Data, error) {
	packet, err := ms.router.recvPacket("", PTData, timeout)
	if err != nil {
		return "", nil, err
	}
	return packet.Src, packet.GetData(), nil
}

// RecvFile is function for receive File packet with timeout (ms)
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvFile(timeout int64) (string, *File, error) {
	packet, err := ms.router.recvPacket("", PTFile, timeout)
	if err != nil {
		return "", nil, err
	}
	return packet.Src, packet.GetFile(), nil
}

// RecvText is function for receive Text packet with timeout (ms)
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvText(timeout int64) (string, *Text, error) {
	packet, err := ms.router.recvPacket("", PTText, timeout)
	if err != nil {
		return "", nil, err
	}
	return packet.Src, packet.GetText(), nil
}

// RecvMsg is function for receive Msg packet with timeout (ms)
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvMsg(timeout int64) (string, *Msg, error) {
	packet, err := ms.router.recvPacket("", PTMsg, timeout)
	if err != nil {
		return "", nil, err
	}
	return packet.Src, packet.GetMsg(), nil
}

// RecvDataFrom is function for receive Data packet with timeout (ms)
// If src equals empty string so there will use receive from all agents and tokens
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvDataFrom(src string, timeout int64) (*Data, error) {
	packet, err := ms.router.recvPacket(src, PTData, timeout)
	if err != nil {
		return nil, err
	}
	return packet.GetData(), nil
}

// RecvFileFrom is function for receive File packet with timeout (ms)
// If src equals empty string so there will use receive from all agents and tokens
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvFileFrom(src string, timeout int64) (*File, error) {
	packet, err := ms.router.recvPacket(src, PTFile, timeout)
	if err != nil {
		return nil, err
	}
	return packet.GetFile(), nil
}

// RecvTextFrom is function for receive Text packet with timeout (ms)
// If src equals empty string so there will use receive from all agents and tokens
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvTextFrom(src string, timeout int64) (*Text, error) {
	packet, err := ms.router.recvPacket(src, PTText, timeout)
	if err != nil {
		return nil, err
	}
	return packet.GetText(), nil
}

// RecvMsgFrom is function for receive Msg packet with timeout (ms)
// If src equals empty string so there will use receive from all agents and tokens
// If timeout equals -1 value so there will permanently blocked this function
func (ms *moduleSocket) RecvMsgFrom(src string, timeout int64) (*Msg, error) {
	packet, err := ms.router.recvPacket(src, PTMsg, timeout)
	if err != nil {
		return nil, err
	}
	return packet.GetMsg(), nil
}

// recvPacket is function for receiving from agent and serving packet to target module
// Result is the success of packet processing otherwise will raise error
func (ms *moduleSocket) recvPacket(packet *Packet) error {
	if ms.IDefaultReceiver != nil {
		switch packet.PType {
		case PTFile:
			// TODO: need implement file builder
			fallthrough
		case PTText:
			fallthrough
		case PTMsg:
			if err := ms.DefaultRecvPacket(packet); err != nil {
				return err
			}
		}
	}
	ms.router.routePacket(packet)
	return nil
}

// sendPacket use in ms API to send data to agent
func (ms *moduleSocket) sendPacket(dst string, pType PacketType, payload interface{}) error {
	packet := &Packet{
		Module:  ms.GetName(),
		Dst:     dst,
		TS:      time.Now().Unix(),
		PType:   pType,
		Payload: payload,
	}
	if ms.IProtoIO == nil {
		return errors.New("undefined interface ProtoIO")
	}
	if ms.HasIMCTokenFormat(dst) {
		dstms := ms.GetIMCModuleSocket(dst)
		if dstms == nil {
			return errors.New("destination is unreachable")
		}
		packet.Module = dstms.GetName()
		packet.Src = ms.GetIMCToken()
		return dstms.recvPacket(packet)
	}
	return ms.IProtoIO.sendPacket(packet)
}

// SendData use in ms API to send data to agent
func (ms *moduleSocket) SendDataTo(dst string, data *Data) error {
	return ms.sendPacket(dst, PTData, data)
}

// SendFile use in ms API to send file to agent
func (ms *moduleSocket) SendFileTo(dst string, file *File) error {
	// TODO: need implement file reader
	return ms.sendPacket(dst, PTFile, file)
}

// SendText use in ms API to send text to agent
func (ms *moduleSocket) SendTextTo(dst string, text *Text) error {
	return ms.sendPacket(dst, PTText, text)
}

// SendMsg use in ms API to send message to agent
func (ms *moduleSocket) SendMsgTo(dst string, msg *Msg) error {
	return ms.sendPacket(dst, PTMsg, msg)
}
