package vxproto

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/vxcontrol/vxcommon/protocol"
)

// ControlMessageType is type of message that used for modules communication
type ControlMessageType int32

// Enumerate control message types
const (
	AgentConnected    ControlMessageType = 0
	AgentDisconnected ControlMessageType = 1
	StopModule        ControlMessageType = 2
)

var controlMsgTypeName = map[int32]string{
	0: "AgentConnected",
	1: "AgentDisconnected",
	2: "StopModule",
}

var controlMsgTypeValue = map[string]int32{
	"AgentConnected":    0,
	"AgentDisconnected": 1,
	"StopModule":        2,
}

func (cmt ControlMessageType) String() string {
	if str, ok := controlMsgTypeName[int32(cmt)]; ok {
		return str
	}

	return "unknown"
}

// MarshalJSON using for convert from ControlMessageType to JSON
func (cmt *ControlMessageType) MarshalJSON() ([]byte, error) {
	if str, ok := controlMsgTypeName[int32(*cmt)]; ok {
		return []byte(`"` + str + `"`), nil
	}

	return nil, errors.New("can't marshal ControlMessageType")
}

// UnmarshalJSON using for convert from JSON to ControlMessageType
func (cmt *ControlMessageType) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)

	if name, ok := controlMsgTypeValue[str]; ok {
		*cmt = ControlMessageType(name)
		return nil
	}

	return errors.New("can't unmarshal ControlMessageType")
}

// ControlMessage is struct that used for modules communication
type ControlMessage struct {
	AgentInfo *AgentInfo         `json:"agent,omitempty"`
	MsgType   ControlMessageType `json:"type"`
}

// fromPB is converter from Protobuf to ControlMessage structure (dosen't use)
func (cm *ControlMessage) fromPB(content *protocol.Packet_Content) *ControlMessage {
	return cm
}

// toPB is converter from ControlMessage to Protobuf structure (dosen't use)
func (cm *ControlMessage) toPB() *protocol.Packet_Content {
	return nil
}

// Data is simple protocol type that used for custom data sending
type Data struct {
	Data []byte `json:"data"`
}

// fromPB is converter from Protobuf to Data structure
func (data *Data) fromPB(content *protocol.Packet_Content) *Data {
	data.Data = content.GetData()

	return data
}

// toPB is converter from Data to Protobuf structure
func (data *Data) toPB() *protocol.Packet_Content {
	content := &protocol.Packet_Content{
		Data: data.Data,
		Type: protocol.Packet_Content_DATA.Enum(),
	}

	return content
}

// File is simple protocol type that used for file content sending
type File struct {
	Data []byte `json:"data,omitempty"`
	Name string `json:"name"`
	Path string `json:"path"`
	Uniq string `json:"uniq"`
}

// fromPB is converter from Protobuf to File structure
func (file *File) fromPB(content *protocol.Packet_Content) *File {
	file.Data = content.GetData()
	file.Name = content.GetName()
	file.Uniq = content.GetUniq()

	return file
}

// toPB is converter from File to Protobuf structure
func (file *File) toPB() *protocol.Packet_Content {
	content := &protocol.Packet_Content{
		Data: file.Data,
		Name: &file.Name,
		Uniq: &file.Uniq,
		Type: protocol.Packet_Content_FILE.Enum(),
	}

	return content
}

// Text is simple protocol type that used for text content sending
type Text struct {
	Data []byte `json:"data"`
	Name string `json:"name"`
}

// fromPB is converter from Protobuf to Text structure
func (text *Text) fromPB(content *protocol.Packet_Content) *Text {
	text.Data = content.GetData()
	text.Name = content.GetName()

	return text
}

// toPB is converter from Text to Protobuf structure
func (text *Text) toPB() *protocol.Packet_Content {
	content := &protocol.Packet_Content{
		Data: text.Data,
		Name: &text.Name,
		Type: protocol.Packet_Content_TEXT.Enum(),
	}

	return content
}

// MsgType is message type which will be common log types
type MsgType int32

// Enumerate received (log) message types
const (
	MTDebug   MsgType = 0
	MTInfo    MsgType = 1
	MTWarning MsgType = 2
	MTError   MsgType = 3
)

var msgTypeName = map[int32]string{
	0: "DEBUG",
	1: "INFO",
	2: "WARNING",
	3: "ERROR",
}

var msgTypeValue = map[string]int32{
	"DEBUG":   0,
	"INFO":    1,
	"WARNING": 2,
	"ERROR":   3,
}

func (mt MsgType) String() string {
	if str, ok := msgTypeName[int32(mt)]; ok {
		return str
	}

	return "unknown"
}

// MarshalJSON using for convert from MsgType to JSON
func (mt *MsgType) MarshalJSON() ([]byte, error) {
	if str, ok := msgTypeName[int32(*mt)]; ok {
		return []byte(`"` + str + `"`), nil
	}

	return nil, errors.New("can't marshal MsgType")
}

// UnmarshalJSON using for convert from JSON to MsgType
func (mt *MsgType) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)

	if name, ok := msgTypeValue[str]; ok {
		*mt = MsgType(name)
		return nil
	}

	return errors.New("can't unmarshal MsgType")
}

// Msg is simple protocol type that used for message sending
type Msg struct {
	Data  []byte  `json:"data"`
	MType MsgType `json:"msg_type"`
}

// fromPB is converter from Protobuf to Msg structure
func (msg *Msg) fromPB(content *protocol.Packet_Content) *Msg {
	msg.Data = content.GetData()
	msg.MType = MsgType(content.GetMsgType())

	return msg
}

// toPB is converter from Msg to Protobuf structure
func (msg *Msg) toPB() *protocol.Packet_Content {
	content := &protocol.Packet_Content{
		Data:    msg.Data,
		MsgType: protocol.Packet_Content_MsgType(msg.MType).Enum(),
		Type:    protocol.Packet_Content_MSG.Enum(),
	}

	return content
}

// PacketType is packet type which will be common types or control
type PacketType int32

// Enumerate packet types
const (
	PTData    PacketType = 0
	PTFile    PacketType = 1
	PTText    PacketType = 2
	PTMsg     PacketType = 3
	PTControl PacketType = 4
)

var packetTypeName = map[int32]string{
	0: "Data",
	1: "File",
	2: "Text",
	3: "Msg",
	4: "ControlMsg",
}

var packetTypeValue = map[string]int32{
	"Data":       0,
	"File":       1,
	"Text":       2,
	"Msg":        3,
	"ControlMsg": 4,
}

func (pt PacketType) String() string {
	if str, ok := packetTypeName[int32(pt)]; ok {
		return str
	}

	return "unknown"
}

// MarshalJSON using for convert from PacketType to JSON
func (pt *PacketType) MarshalJSON() ([]byte, error) {
	if str, ok := packetTypeName[int32(*pt)]; ok {
		return []byte(`"` + str + `"`), nil
	}

	return nil, errors.New("can't marshal PacketType")
}

// UnmarshalJSON using for convert from JSON to PacketType
func (pt *PacketType) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)

	if name, ok := packetTypeValue[str]; ok {
		*pt = PacketType(name)
		return nil
	}

	return errors.New("can't unmarshal PacketType")
}

// Packet is common struct that hide type of true received packet
type Packet struct {
	Module  string      `json:"module"`
	Src     string      `json:"source"`
	Dst     string      `json:"destination"`
	TS      int64       `json:"timestamp"`
	PType   PacketType  `json:"type"`
	Payload interface{} `json:"content"`
}

// GetData is function that cast payload to Data struct and return it
func (p *Packet) GetData() *Data {
	return p.Payload.(*Data)
}

// GetFile is function that cast payload to File struct and return it
func (p *Packet) GetFile() *File {
	return p.Payload.(*File)
}

// GetText is function that cast payload to Text struct and return it
func (p *Packet) GetText() *Text {
	return p.Payload.(*Text)
}

// GetMsg is function that cast payload to Msg struct and return it
func (p *Packet) GetMsg() *Msg {
	return p.Payload.(*Msg)
}

// GetControlMsg is function that cast payload to ControlMessage struct and return it
func (p *Packet) GetControlMsg() *ControlMessage {
	return p.Payload.(*ControlMessage)
}

// fromPB is converter from Protobuf to Packet structure
func (p *Packet) fromPB(packet *protocol.Packet) (*Packet, error) {
	p.Module = packet.GetModule()
	p.Src = packet.GetSource()
	p.Dst = packet.GetDestination()
	p.TS = packet.GetTimestamp()
	content := packet.GetContent()
	switch content.GetType() {
	case protocol.Packet_Content_DATA:
		p.PType = PTData
		p.Payload = (&Data{}).fromPB(content)
	case protocol.Packet_Content_FILE:
		p.PType = PTFile
		p.Payload = (&File{}).fromPB(content)
	case protocol.Packet_Content_TEXT:
		p.PType = PTText
		p.Payload = (&Text{}).fromPB(content)
	case protocol.Packet_Content_MSG:
		p.PType = PTMsg
		p.Payload = (&Msg{}).fromPB(content)
	default:
		return nil, errors.New("unknown packet type")
	}

	return p, nil
}

// fromBytesPB is converter from bytes array (Protobuf) to Packet structure
func (p *Packet) fromBytesPB(data []byte) (*Packet, error) {
	var packet protocol.Packet
	err := proto.Unmarshal(data, &packet)
	if err != nil {
		return nil, errors.New("failed parsing of packet: " + err.Error())
	}

	return p.fromPB(&packet)
}

// fromBytesJSON is converter from bytes array (JSON) to Packet structure
func (p *Packet) fromBytesJSON(data []byte) (*Packet, error) {
	if err := json.Unmarshal(data, p); err != nil {
		return nil, err
	}

	var ok bool
	var content *json.RawMessage
	var tp map[string]*json.RawMessage
	if err := json.Unmarshal(data, &tp); err != nil {
		return nil, err
	}
	if content, ok = tp["content"]; !ok {
		return nil, errors.New("field content not found")
	}

	switch p.PType {
	case PTData:
		var data Data
		if err := json.Unmarshal(*content, &data); err != nil {
			return nil, err
		}
		p.Payload = &data
	case PTFile:
		var file File
		if err := json.Unmarshal(*content, &file); err != nil {
			return nil, err
		}
		p.Payload = &file
	case PTText:
		var text Text
		if err := json.Unmarshal(*content, &text); err != nil {
			return nil, err
		}
		p.Payload = &text
	case PTMsg:
		var msg Msg
		if err := json.Unmarshal(*content, &msg); err != nil {
			return nil, err
		}
		p.Payload = &msg
	default:
		return nil, errors.New("unknown packet type")
	}

	return p, nil
}

// toPB is converter from Packet to Protobuf structure
func (p *Packet) toPB() (*protocol.Packet, error) {
	var content *protocol.Packet_Content
	switch p.PType {
	case PTData:
		content = p.Payload.(*Data).toPB()
	case PTFile:
		content = p.Payload.(*File).toPB()
	case PTText:
		content = p.Payload.(*Text).toPB()
	case PTMsg:
		content = p.Payload.(*Msg).toPB()
	default:
		return nil, errors.New("unknown packet type")
	}

	return &protocol.Packet{
		Module:      &p.Module,
		Source:      &p.Src,
		Destination: &p.Dst,
		Timestamp:   &p.TS,
		Content:     content,
	}, nil
}

// toBytesPB is converter from Packet structure to bytes array (Protobuf)
func (p *Packet) toBytesPB() ([]byte, error) {
	packet, err := p.toPB()
	if err != nil {
		return nil, err
	}

	data, err := proto.Marshal(packet)
	if err != nil {
		return nil, errors.New("failed building of packet: " + err.Error())
	}

	return data, nil
}

// toBytesJSON is converter from Packet structure to bytes array (JSON)
func (p *Packet) toBytesJSON() ([]byte, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return nil, errors.New("failed building of packet: " + err.Error())
	}

	return data, nil
}
