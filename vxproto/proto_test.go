package vxproto

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vxcontrol/vxcommon/agent"
)

type FakeSocket struct {
	isConnected bool
	RecvChan    chan []byte
	SendChan    chan []byte
	CloseChan   chan struct{}
}

func (fc *FakeSocket) Read() (data []byte, err error) {
	select {
	case msg := <-fc.RecvChan:
		return msg, nil
	case <-fc.CloseChan:
		return nil, errors.New("connection was force closed")
	}
}

func (fc *FakeSocket) Write(data []byte) error {
	select {
	case fc.SendChan <- data:
		return nil
	case <-fc.CloseChan:
		return errors.New("connection was force closed")
	}
}

func (fc *FakeSocket) Close() error {
	if fc.isConnected {
		fc.isConnected = false
		select {
		case fc.CloseChan <- struct{}{}:
		default:
		}
	} else {
		return errors.New("connection has already closed")
	}
	return nil
}

type FakeMainModule struct{}

func (mModule *FakeMainModule) DefaultRecvPacket(packet *Packet) error {
	return nil
}

func (mModule *FakeMainModule) HasAgentIDValid(agentID string, agentType AgentType) bool {
	return true
}

func (mModule *FakeMainModule) HasAgentInfoValid(agentID string, info *agent.Information) bool {
	return true
}

func (mModule *FakeMainModule) OnConnect(socket IAgentSocket) error {
	return nil
}

// getRef is function for returning referense of string
func getRef(str string) *string {
	return &str
}

const (
	agentID     = "12345678901234567890123456789012"
	agentToken  = "1234567890123456789012345678901234567890"
	serverToken = "0123456789012345678901234567890123456789"
)

func makeAgentSocket(vxp *vxProto) *agentSocket {
	seconds := time.Now().Unix()
	return &agentSocket{
		id:  agentID,
		ip:  "192.168.1.1",
		src: serverToken,
		at:  VXAgent,
		auth: &AuthenticationData{
			req: &agent.AuthenticationRequest{
				Timestamp: &seconds,
				Atoken:    getRef(""),
			},
			resp: &agent.AuthenticationResponse{
				Atoken: getRef(agentToken),
				Stoken: getRef(serverToken),
			},
		},
		info: &agent.Information{
			Os: &agent.Information_OS{
				Type: getRef("linux"),
				Name: getRef("Ubuntu 16.04"),
				Arch: getRef("amd64"),
			},
			User: &agent.Information_User{
				Name:  getRef("root"),
				Group: getRef("root"),
			},
		},
		mRecv:      &sync.Mutex{},
		mSend:      &sync.Mutex{},
		IVaildator: vxp,
		IProtoIO:   vxp,
	}
}

func makePacket(pType PacketType, payload interface{}) *Packet {
	seconds := time.Now().Unix()
	packet := &Packet{
		Module:  "test",
		Src:     agentToken,
		Dst:     serverToken,
		TS:      seconds,
		PType:   pType,
		Payload: payload,
	}
	return packet
}

func makePacketData(data []byte) *Packet {
	return makePacket(PTData, &Data{Data: data})
}

func makePacketText(data []byte, name string) *Packet {
	return makePacket(PTText, &Text{Data: data, Name: name})
}

func makePacketMsg(data []byte, mType MsgType) *Packet {
	return makePacket(PTMsg, &Msg{Data: data, MType: mType})
}

func parsePacket(data []byte, pType PacketType) (*Packet, error) {
	packet, err := (&Packet{}).fromBytesPB(data)
	if err != nil {
		return nil, err
	}

	if packet.Module != "test" {
		return nil, errors.New("invalid module name")
	}

	if packet.Dst != agentToken {
		return nil, errors.New("invalid destination")
	}

	if packet.PType != pType {
		return nil, errors.New("invalid packet type")
	}

	return packet, nil
}

func parsePacketData(data []byte) ([]byte, error) {
	packet, err := parsePacket(data, PTData)
	if err != nil {
		return nil, err
	}

	return packet.GetData().Data, nil
}

func parsePacketText(data []byte) ([]byte, string, error) {
	packet, err := parsePacket(data, PTText)
	if err != nil {
		return nil, "", err
	}

	return packet.GetText().Data, packet.GetText().Name, nil
}

func parsePacketMsg(data []byte) ([]byte, MsgType, error) {
	packet, err := parsePacket(data, PTMsg)
	if err != nil {
		return nil, 0, err
	}

	return packet.GetMsg().Data, packet.GetMsg().MType, nil
}

func randString(nchars int) string {
	rbytes := make([]byte, nchars)
	if _, err := rand.Read(rbytes); err != nil {
		return ""
	}

	return hex.EncodeToString(rbytes)
}

func TestNewProto(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	if err := proto.Close(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestTokenValidation(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}

	agentSocket := makeAgentSocket(proto)
	token := agentSocket.NewToken()
	if token == "" {
		t.Fatal("Failed generate token")
	}
	if !agentSocket.HasTokenCRCValid(token) {
		t.Fatal("Failed validate CRC32 of token")
	}
	if !agentSocket.HasAgentIDValid(token, VXAgent) {
		t.Fatal("Failed validate token")
	}

	if err := proto.Close(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestNewModule(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	if moduleSocket := proto.NewModule("test", agentID); moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			t.Fatal("Failed add Module Socket object")
		}
		if !proto.DelModule(moduleSocket) {
			t.Fatal("Failed delete Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize Module Socket object")
	}
	if err := proto.Close(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestLinkAgentToModule(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			t.Fatal("Failed add Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			t.Fatal("Failed delete Module Socket object")
		}
	}()

	var wg sync.WaitGroup
	agentSocket := makeAgentSocket(proto)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if !proto.addAgent(agentSocket) {
			t.Error("Failed add Agent Socket object")
		}
		defer func() {
			if !proto.delAgent(agentSocket) {
				t.Error("Failed delete Agent Socket object")
			}
		}()
	}()

	var packet *Packet
	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentConnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed connect Agent to proto")
	}

	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentDisconnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed disconnect Agent from proto")
	}

	wg.Wait()
}

func TestAPISendFromAgentToModule(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			t.Fatal("Failed add Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			t.Fatal("Failed delete Module Socket object")
		}
	}()

	var wg sync.WaitGroup
	testData := []byte("test data message")
	textName := "test_text_name"
	msgType := MTDebug
	agentSocket := makeAgentSocket(proto)
	recvSyncChan := make(chan struct{})
	sendSyncChan := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		packetData, err := moduleSocket.RecvDataFrom(agentToken, -1)
		if err != nil {
			t.Error("Failed receive data packet")
		}
		if !bytes.Equal(packetData.Data, testData) {
			t.Error("Failed compare of received data")
		}
		recvSyncChan <- struct{}{}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		packetText, err := moduleSocket.RecvTextFrom(agentToken, -1)
		if err != nil {
			t.Error("Failed receive text packet")
		}
		if !bytes.Equal(packetText.Data, testData) || packetText.Name != textName {
			t.Error("Failed compare of received text")
		}
		recvSyncChan <- struct{}{}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		packetMsg, err := moduleSocket.RecvMsgFrom(agentToken, -1)
		if err != nil {
			t.Error("Failed receive message packet")
		}
		if !bytes.Equal(packetMsg.Data, testData) || packetMsg.MType != msgType {
			t.Error("Failed compare of received message")
		}
		recvSyncChan <- struct{}{}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if !proto.addAgent(agentSocket) {
			t.Error("Failed add Agent Socket object")
		}
		defer func() {
			if !proto.delAgent(agentSocket) {
				t.Error("Failed delete Agent Socket object")
			}
		}()

		addPacket := func(packet *Packet) {
			if err := proto.recvPacket(packet); err != nil {
				t.Error("Failed to receive packet:", err.Error())
			}
		}

		time.Sleep(100 * time.Millisecond)
		addPacket(makePacketData(testData))
		addPacket(makePacketText(testData, textName))
		addPacket(makePacketMsg(testData, msgType))
		for i := 0; i < 3; i++ {
			<-recvSyncChan
		}
		<-sendSyncChan

		addPacket(makePacketData(testData))
		addPacket(makePacketText(testData, textName))
		addPacket(makePacketMsg(testData, msgType))
		<-sendSyncChan
	}()

	var packet *Packet
	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentConnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed connect Agent to proto")
	}
	sendSyncChan <- struct{}{}

	// TODO: Need implement File API in testing
	packet = <-moduleSocket.GetReceiver()
	if !bytes.Equal(packet.GetData().Data, testData) {
		t.Fatal("Failed compare of received data")
	}
	packet = <-moduleSocket.GetReceiver()
	if !bytes.Equal(packet.GetText().Data, testData) || packet.GetText().Name != textName {
		t.Fatal("Failed compare of received text")
	}
	packet = <-moduleSocket.GetReceiver()
	if !bytes.Equal(packet.GetMsg().Data, testData) || packet.GetMsg().MType != msgType {
		t.Fatal("Failed compare of received message")
	}

	sendSyncChan <- struct{}{}

	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentDisconnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed disconnect Agent from proto")
	}

	wg.Wait()
}

func TestFakeSocketSendFromAgentToModule(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			t.Fatal("Failed add Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			t.Fatal("Failed delete Module Socket object")
		}
	}()

	var wg sync.WaitGroup
	testData := []byte("test data message")
	textName := "test_text_name"
	msgType := MTDebug
	agentSocket := makeAgentSocket(proto)
	recvSyncChan := make(chan struct{})
	quitSyncChan := make(chan struct{})

	fakeSocket := &FakeSocket{
		isConnected: true,
		RecvChan:    make(chan []byte),
		SendChan:    make(chan []byte),
		CloseChan:   make(chan struct{}),
	}
	agentSocket.IConnection = fakeSocket

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-recvSyncChan

		if !proto.addAgent(agentSocket) {
			t.Error("Failed add Agent Socket object")
		}
		defer func() {
			if !proto.delAgent(agentSocket) {
				t.Error("Failed delete Agent Socket object")
			}
		}()
		defer func() {
			if fakeSocket.Close() != nil {
				t.Error("Failed close Fake Socket object")
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for ix := 0; ix < 3; ix++ {
				err := agentSocket.recvPacket()
				if err != nil && fakeSocket.isConnected {
					t.Error("Failed to receive packet:", err.Error())
				}
			}
		}()

		addPacket := func(packet *Packet) {
			packetData, err := packet.toBytesPB()
			if err != nil {
				t.Error("Failed to make packet:", err.Error())
			}

			fakeSocket.RecvChan <- packetData
		}

		addPacket(makePacketData(testData))
		addPacket(makePacketText(testData, textName))
		addPacket(makePacketMsg(testData, msgType))

		<-quitSyncChan
	}()

	recvSyncChan <- struct{}{}
	var packet *Packet
	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentConnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed connect Agent to proto")
	}

	// TODO: Need implement File API in testing
	packet = <-moduleSocket.GetReceiver()
	if !bytes.Equal(packet.GetData().Data, testData) {
		t.Fatal("Failed compare of received data")
	}
	packet = <-moduleSocket.GetReceiver()
	if !bytes.Equal(packet.GetText().Data, testData) || packet.GetText().Name != textName {
		t.Fatal("Failed compare of received text")
	}
	packet = <-moduleSocket.GetReceiver()
	if !bytes.Equal(packet.GetMsg().Data, testData) || packet.GetMsg().MType != msgType {
		t.Fatal("Failed compare of received message")
	}

	quitSyncChan <- struct{}{}
	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentDisconnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed disconnect Agent from proto")
	}

	wg.Wait()
}

func TestFakeSocketSendFromModuleToAgent(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			t.Fatal("Failed add Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			t.Fatal("Failed delete Module Socket object")
		}
	}()

	var wg sync.WaitGroup
	testData := []byte("test data message")
	textName := "test_text_name"
	msgType := MTDebug
	agentSocket := makeAgentSocket(proto)

	fakeSocket := &FakeSocket{
		isConnected: true,
		RecvChan:    make(chan []byte),
		SendChan:    make(chan []byte),
		CloseChan:   make(chan struct{}),
	}
	agentSocket.IConnection = fakeSocket

	wg.Add(1)
	go func() {
		defer wg.Done()
		if !proto.addAgent(agentSocket) {
			t.Error("Failed add Agent Socket object")
		}
		defer func() {
			if !proto.delAgent(agentSocket) {
				t.Error("Failed delete Agent Socket object")
			}
		}()
		defer func() {
			if fakeSocket.Close() != nil {
				t.Error("Failed close Fake Socket object")
			}
		}()

		var packetData []byte
		packetData = <-fakeSocket.SendChan
		recvData, err := parsePacketData(packetData)
		if err != nil {
			t.Error("Failed to receive data packet:", err.Error())
		}
		if !bytes.Equal(recvData, testData) {
			t.Error("Failed compare of received data")
		}

		packetData = <-fakeSocket.SendChan
		recvData, recvName, err := parsePacketText(packetData)
		if err != nil {
			t.Error("Failed to receive text packet:", err.Error())
		}
		if !bytes.Equal(recvData, testData) || recvName != textName {
			t.Error("Failed compare of received text")
		}

		packetData = <-fakeSocket.SendChan
		recvData, recvMsgType, err := parsePacketMsg(packetData)
		if err != nil {
			t.Error("Failed to receive message packet:", err.Error())
		}
		if !bytes.Equal(recvData, testData) || recvMsgType != msgType {
			t.Error("Failed compare of received message")
		}
	}()

	var packet *Packet
	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentConnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed connect Agent to proto")
	}

	// TODO: Need implement File API in testing
	data := &Data{Data: testData}
	if err := moduleSocket.SendDataTo(agentToken, data); err != nil {
		t.Fatal(err.Error())
	}
	text := &Text{Data: testData, Name: textName}
	if err := moduleSocket.SendTextTo(agentToken, text); err != nil {
		t.Fatal(err.Error())
	}
	msg := &Msg{Data: testData, MType: msgType}
	if err := moduleSocket.SendMsgTo(agentToken, msg); err != nil {
		t.Fatal(err.Error())
	}

	packet = <-moduleSocket.GetReceiver()
	if packet.GetControlMsg().MsgType != AgentDisconnected || packet.GetControlMsg().AgentInfo.ID != agentID {
		t.Fatal("Failed disconnect Agent from proto")
	}

	wg.Wait()
}

func TestIMCSendRecvPackets(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	module1Token := proto.MakeIMCToken(agentID, "test1")
	moduleSocket1 := proto.NewModule("test1", agentID)
	if moduleSocket1 != nil {
		if !proto.AddModule(moduleSocket1) {
			t.Fatal("Failed add first Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize first Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket1) {
			t.Fatal("Failed delete first Module Socket object")
		}
	}()

	module2Token := proto.MakeIMCToken(agentID, "test2")
	moduleSocket2 := proto.NewModule("test2", agentID)
	if moduleSocket2 != nil {
		if !proto.AddModule(moduleSocket2) {
			t.Fatal("Failed add second Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize second Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket2) {
			t.Fatal("Failed delete second Module Socket object")
		}
	}()

	var wg sync.WaitGroup
	testData := []byte("test data message")
	textName := "test_text_name"
	msgType := MTDebug
	recvSyncChan := make(chan struct{})
	sendSyncChan := make(chan struct{})

	batchReceive := func(ms IModuleSocket, src, dst string) {
		checkRoutingInfo := func(packet *Packet) {
			if packet.Src != src || packet.Dst != dst {
				t.Fatal("Failed routing info of received packet on " + ms.GetName())
			}
		}

		var packet *Packet
		// TODO: Need implement File API in testing
		packet = <-ms.GetReceiver()
		if !bytes.Equal(packet.GetData().Data, testData) {
			t.Fatal("Failed compare of received data on " + ms.GetName())
		}
		checkRoutingInfo(packet)
		packet = <-ms.GetReceiver()
		if !bytes.Equal(packet.GetText().Data, testData) || packet.GetText().Name != textName {
			t.Fatal("Failed compare of received text on " + ms.GetName())
		}
		checkRoutingInfo(packet)
		packet = <-ms.GetReceiver()
		if !bytes.Equal(packet.GetMsg().Data, testData) || packet.GetMsg().MType != msgType {
			t.Fatal("Failed compare of received message on " + ms.GetName())
		}
		checkRoutingInfo(packet)
	}

	batchSend := func(ms IModuleSocket, dst string) {
		// TODO: Need implement File API in testing
		if err := ms.SendDataTo(dst, &Data{Data: testData}); err != nil {
			t.Fatal("Failed send data packet to modude via imc:", err.Error())
		}
		if err := ms.SendTextTo(dst, &Text{Data: testData, Name: textName}); err != nil {
			t.Fatal("Failed send text packet to modude via imc:", err.Error())
		}
		if err := ms.SendMsgTo(dst, &Msg{Data: testData, MType: msgType}); err != nil {
			t.Fatal("Failed send message packet to modude via imc:", err.Error())
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		sendSyncChan <- struct{}{}
		batchReceive(moduleSocket1, module2Token, module1Token)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-recvSyncChan
		batchSend(moduleSocket1, module2Token)
	}()

	recvSyncChan <- struct{}{}
	batchReceive(moduleSocket2, module1Token, module2Token)
	<-sendSyncChan
	batchSend(moduleSocket2, module1Token)

	wg.Wait()
}

func TestIMCSendToUnknownDestination(t *testing.T) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		t.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			t.Fatal("Failed add Module Socket object")
		}
	} else {
		t.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			t.Fatal("Failed delete Module Socket object")
		}
	}()

	testData := []byte("test data message")
	textName := "test_text_name"
	msgType := MTDebug
	dst := proto.MakeIMCToken(agentID, "unknown")

	// TODO: Need implement File API in testing
	if err := moduleSocket.SendDataTo(dst, &Data{Data: testData}); err == nil {
		t.Fatal("Failed send data packet to modude via imc:", err.Error())
	}
	if err := moduleSocket.SendTextTo(dst, &Text{Data: testData, Name: textName}); err == nil {
		t.Fatal("Failed send text packet to modude via imc:", err.Error())
	}
	if err := moduleSocket.SendMsgTo(dst, &Msg{Data: testData, MType: msgType}); err == nil {
		t.Fatal("Failed send message packet to modude via imc:", err.Error())
	}
}

func BenchmarkStabCreatingProto(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proto := New(&FakeMainModule{}).(*vxProto)
		if proto == nil {
			b.Fatal("Failed initialize VXProto object")
		}
		if err := proto.Close(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkStabCreatingModule(b *testing.B) {
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		b.Fatal("Failed initialize VXProto object")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			modName := randString(10)
			if moduleSocket := proto.NewModule(modName, agentID); moduleSocket != nil {
				if !proto.AddModule(moduleSocket) {
					b.Fatal("Failed add Module Socket object")
				}
				if !proto.DelModule(moduleSocket) {
					b.Fatal("Failed delete Module Socket object")
				}
			} else {
				b.Fatal("Failed initialize Module Socket object")
			}
		}
	})

	if err := proto.Close(); err != nil {
		b.Fatal(err.Error())
	}
}

func BenchmarkStabLinkAgentToModule(b *testing.B) {
	var cntAgents, cntCon, cntDis int64
	var wg sync.WaitGroup
	quit := make(chan struct{})
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		b.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			b.Fatal("Failed add Module Socket object")
		}
	} else {
		b.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			b.Fatal("Failed delete Module Socket object")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			case packet := <-moduleSocket.GetReceiver():
				if packet.GetControlMsg().AgentInfo.ID != agentID {
					b.Error("Failed to get Agent ID from packet proto")
				}
				if packet.GetControlMsg().MsgType == AgentConnected {
					atomic.AddInt64(&cntCon, 1)
					continue
				}
				if packet.GetControlMsg().MsgType == AgentDisconnected {
					atomic.AddInt64(&cntDis, 1)
					continue
				}
				b.Error("Failed to get Message Type from packet proto")
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			agentSocket := makeAgentSocket(proto)
			agentSocket.src = randString(20)
			agentSocket.auth.resp.Stoken = &agentSocket.src
			agentToken := randString(20)
			agentSocket.auth.req.Atoken = &agentToken
			agentSocket.auth.resp.Atoken = &agentToken
			if !proto.addAgent(agentSocket) {
				b.Fatal("Failed add Agent Socket object")
			}
			if !proto.delAgent(agentSocket) {
				b.Fatal("Failed delete Agent Socket object")
			}
			atomic.AddInt64(&cntAgents, 1)
		}
	})
	b.StopTimer()

	for atomic.LoadInt64(&cntCon) != atomic.LoadInt64(&cntAgents) {
		runtime.Gosched()
	}
	for atomic.LoadInt64(&cntDis) != atomic.LoadInt64(&cntAgents) {
		runtime.Gosched()
	}

	quit <- struct{}{}
	wg.Wait()
}

func BenchmarkStabAPISendFromAgentToModule(b *testing.B) {
	var cntTotal, cntRecv int64
	var wg sync.WaitGroup
	quit := make(chan struct{})
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		b.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			b.Fatal("Failed add Module Socket object")
		}
	} else {
		b.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			b.Fatal("Failed delete Module Socket object")
		}
	}()

	testData := []byte("test data message")
	testDataLen := int64(len(testData))
	agentSocket := makeAgentSocket(proto)
	addPacket := func(packet *Packet) {
		if err := proto.recvPacket(packet); err != nil {
			b.Fatal("Failed to add packet to the queue:", err.Error())
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			case packet := <-moduleSocket.GetReceiver():
				switch packet.PType {
				case PTData:
					if !bytes.Equal(packet.GetData().Data, testData) {
						b.Error("Failed compare of received data")
					}
					atomic.AddInt64(&cntRecv, 1)
				case PTControl:
					if packet.GetControlMsg().AgentInfo.ID != agentID {
						b.Error("Failed to get Agent ID from packet proto")
					}
					if packet.GetControlMsg().MsgType == AgentConnected {
						continue
					}
					if packet.GetControlMsg().MsgType == AgentDisconnected {
						continue
					}
					b.Error("Failed to get Message Type from packet proto")
				}
			}
		}
	}()

	if !proto.addAgent(agentSocket) {
		b.Fatal("Failed add Agent Socket object")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			addPacket(makePacketData(testData))
			atomic.AddInt64(&cntTotal, 1)
			b.SetBytes(testDataLen)
		}
	})
	b.StopTimer()
	if !proto.delAgent(agentSocket) {
		b.Fatal("Failed delete Agent Socket object")
	}

	for atomic.LoadInt64(&cntRecv) != atomic.LoadInt64(&cntTotal) {
		runtime.Gosched()
	}

	quit <- struct{}{}
	wg.Wait()
}

func BenchmarkStabConnectAndSendFromAgentToModule(b *testing.B) {
	var cntTotal, cntRecv, cntAgents, cntCon, cntDis int64
	var wg sync.WaitGroup
	quit := make(chan struct{})
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		b.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			b.Fatal("Failed add Module Socket object")
		}
	} else {
		b.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			b.Fatal("Failed delete Module Socket object")
		}
	}()

	testData := []byte("test data message")
	testDataLen := int64(len(testData))
	addPacket := func(packet *Packet) {
		if err := proto.recvPacket(packet); err != nil {
			b.Fatal("Failed to add packet to the queue:", err.Error())
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			case packet := <-moduleSocket.GetReceiver():
				switch packet.PType {
				case PTData:
					if !bytes.Equal(packet.GetData().Data, testData) {
						b.Error("Failed compare of received data")
					}
					atomic.AddInt64(&cntRecv, 1)
				case PTControl:
					if packet.GetControlMsg().AgentInfo.ID != agentID {
						b.Error("Failed to get Agent ID from packet proto")
					}
					if packet.GetControlMsg().MsgType == AgentConnected {
						atomic.AddInt64(&cntCon, 1)
						continue
					}
					if packet.GetControlMsg().MsgType == AgentDisconnected {
						atomic.AddInt64(&cntDis, 1)
						continue
					}
					b.Error("Failed to get Message Type from packet proto")
				}
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		agentSocket := makeAgentSocket(proto)
		agentSocket.src = randString(20)
		agentSocket.auth.resp.Stoken = &agentSocket.src
		agentToken := randString(20)
		agentSocket.auth.req.Atoken = &agentToken
		agentSocket.auth.resp.Atoken = &agentToken
		if !proto.addAgent(agentSocket) {
			b.Fatal("Failed add Agent Socket object")
		}
		for pb.Next() {
			packet := makePacketData(testData)
			packet.Src = agentToken
			packet.Dst = agentSocket.src
			addPacket(packet)
			atomic.AddInt64(&cntTotal, 1)
			b.SetBytes(testDataLen)
		}
		if !proto.delAgent(agentSocket) {
			b.Fatal("Failed delete Agent Socket object")
		}
		atomic.AddInt64(&cntAgents, 1)
	})
	b.StopTimer()

	for atomic.LoadInt64(&cntRecv) != atomic.LoadInt64(&cntTotal) {
		runtime.Gosched()
	}
	for atomic.LoadInt64(&cntCon) != atomic.LoadInt64(&cntAgents) {
		runtime.Gosched()
	}
	for atomic.LoadInt64(&cntDis) != atomic.LoadInt64(&cntAgents) {
		runtime.Gosched()
	}

	quit <- struct{}{}
	wg.Wait()
}

func BenchmarkStabFakeSocketSendFromAgentToModule(b *testing.B) {
	var cntTotal, cntAgentRecv, cntModuleRecv int64
	var wg sync.WaitGroup
	quit := make(chan struct{})
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		b.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			b.Fatal("Failed add Module Socket object")
		}
	} else {
		b.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			b.Fatal("Failed delete Module Socket object")
		}
	}()

	testData := []byte("test data message")
	testDataLen := int64(len(testData))
	agentSocket := makeAgentSocket(proto)
	fakeSocket := &FakeSocket{
		isConnected: true,
		RecvChan:    make(chan []byte),
		SendChan:    make(chan []byte),
		CloseChan:   make(chan struct{}),
	}
	agentSocket.IConnection = fakeSocket
	addPacket := func(packet *Packet) {
		packetData, err := packet.toBytesPB()
		if err != nil {
			b.Fatal("Failed to make packet:", err.Error())
		}

		fakeSocket.RecvChan <- packetData
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt64(&cntAgentRecv) != atomic.LoadInt64(&cntTotal) || fakeSocket.isConnected {
			err := agentSocket.recvPacket()
			if err != nil {
				if fakeSocket.isConnected {
					b.Error("Failed to receive packet:", err.Error())
				} else {
					return
				}
			} else {
				atomic.AddInt64(&cntAgentRecv, 1)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			case packet := <-moduleSocket.GetReceiver():
				switch packet.PType {
				case PTData:
					if !bytes.Equal(packet.GetData().Data, testData) {
						b.Error("Failed compare of received data")
					}
					atomic.AddInt64(&cntModuleRecv, 1)
				case PTControl:
					if packet.GetControlMsg().AgentInfo.ID != agentID {
						b.Error("Failed to get Agent ID from packet proto")
					}
					if packet.GetControlMsg().MsgType == AgentConnected {
						continue
					}
					if packet.GetControlMsg().MsgType == AgentDisconnected {
						continue
					}
					b.Error("Failed to get Message Type from packet proto")
				default:
					b.Error("Failed to parse packet type")
				}
			}
		}
	}()

	if !proto.addAgent(agentSocket) {
		b.Fatal("Failed add Agent Socket object")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			addPacket(makePacketData(testData))
			atomic.AddInt64(&cntTotal, 1)
			b.SetBytes(testDataLen)
		}
	})
	b.StopTimer()

	for atomic.LoadInt64(&cntModuleRecv) != atomic.LoadInt64(&cntTotal) {
		runtime.Gosched()
	}
	for atomic.LoadInt64(&cntAgentRecv) != atomic.LoadInt64(&cntTotal) {
		runtime.Gosched()
	}

	if !proto.delAgent(agentSocket) {
		b.Fatal("Failed delete Agent Socket object")
	}
	if fakeSocket.Close() != nil {
		b.Error("Failed close Fake Socket object")
	}

	quit <- struct{}{}
	wg.Wait()
}

func BenchmarkStabFakeSocketSendFromModuleToAgent(b *testing.B) {
	var cntTotal, cntRecv int64
	var wg sync.WaitGroup
	quit := make(chan struct{})
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		b.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			b.Fatal("Failed add Module Socket object")
		}
	} else {
		b.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			b.Fatal("Failed delete Module Socket object")
		}
	}()

	testData := []byte("test data message")
	testDataLen := int64(len(testData))
	agentSocket := makeAgentSocket(proto)
	fakeSocket := &FakeSocket{
		isConnected: true,
		RecvChan:    make(chan []byte),
		SendChan:    make(chan []byte),
		CloseChan:   make(chan struct{}),
	}
	agentSocket.IConnection = fakeSocket

	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt64(&cntRecv) != atomic.LoadInt64(&cntTotal) || fakeSocket.isConnected {
			var packetData []byte
			packetData = <-fakeSocket.SendChan
			if packetData == nil && !fakeSocket.isConnected {
				return
			}
			recvData, err := parsePacketData(packetData)
			if err != nil {
				b.Error("Failed to receive data packet:", err.Error())
			}
			if !bytes.Equal(recvData, testData) {
				b.Error("Failed compare of received data")
			}
			atomic.AddInt64(&cntRecv, 1)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			case packet := <-moduleSocket.GetReceiver():
				switch packet.PType {
				case PTControl:
					if packet.GetControlMsg().AgentInfo.ID != agentID {
						b.Error("Failed to get Agent ID from packet proto")
					}
					if packet.GetControlMsg().MsgType == AgentConnected {
						continue
					}
					if packet.GetControlMsg().MsgType == AgentDisconnected {
						continue
					}
					b.Error("Failed to get Message Type from packet proto")
				default:
					b.Error("Failed to parse packet type")
				}
			}
		}
	}()

	if !proto.addAgent(agentSocket) {
		b.Fatal("Failed add Agent Socket object")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data := &Data{Data: testData}
			if err := moduleSocket.SendDataTo(agentToken, data); err != nil {
				b.Fatal("Failed to send data to fake socket:", err.Error())
			}
			atomic.AddInt64(&cntTotal, 1)
			b.SetBytes(testDataLen)
		}
	})
	b.StopTimer()

	for atomic.LoadInt64(&cntRecv) != atomic.LoadInt64(&cntTotal) {
		runtime.Gosched()
	}

	if !proto.delAgent(agentSocket) {
		b.Fatal("Failed delete Agent Socket object")
	}
	if fakeSocket.Close() != nil {
		b.Error("Failed close Fake Socket object")
	}

	quit <- struct{}{}
	fakeSocket.SendChan <- nil
	wg.Wait()
}

func BenchmarkStabFakeSocketConnectAndSendFromModuleToAgent(b *testing.B) {
	var cntTotal, cntRecv, cntAgents, cntCon, cntDis int64
	var wg sync.WaitGroup
	quit := make(chan struct{})
	proto := New(&FakeMainModule{}).(*vxProto)
	if proto == nil {
		b.Fatal("Failed initialize VXProto object")
	}
	defer proto.Close()

	moduleSocket := proto.NewModule("test", agentID)
	if moduleSocket != nil {
		if !proto.AddModule(moduleSocket) {
			b.Fatal("Failed add Module Socket object")
		}
	} else {
		b.Fatal("Failed initialize Module Socket object")
	}
	defer func() {
		if !proto.DelModule(moduleSocket) {
			b.Fatal("Failed delete Module Socket object")
		}
	}()

	testData := []byte("test data message")
	testDataLen := int64(len(testData))
	fakeSocket := &FakeSocket{
		isConnected: true,
		RecvChan:    make(chan []byte),
		SendChan:    make(chan []byte),
		CloseChan:   make(chan struct{}),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt64(&cntRecv) != atomic.LoadInt64(&cntTotal) || fakeSocket.isConnected {
			var packetData []byte
			packetData = <-fakeSocket.SendChan
			if packetData == nil && !fakeSocket.isConnected {
				return
			}
			packet, err := (&Packet{}).fromBytesPB(packetData)
			if err != nil {
				b.Error("Failed to receive data packet:", err.Error())
			}
			if packet.Module != "test" {
				b.Error("Failed to match module name")
			}
			if packet.PType != PTData {
				b.Error("Failed to match packet type")
			}
			if !bytes.Equal(packet.GetData().Data, testData) {
				b.Error("Failed compare of received data")
			}
			atomic.AddInt64(&cntRecv, 1)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			case packet := <-moduleSocket.GetReceiver():
				switch packet.PType {
				case PTControl:
					if packet.GetControlMsg().AgentInfo.ID != agentID {
						b.Error("Failed to get Agent ID from packet proto")
					}
					if packet.GetControlMsg().MsgType == AgentConnected {
						atomic.AddInt64(&cntCon, 1)
						continue
					}
					if packet.GetControlMsg().MsgType == AgentDisconnected {
						atomic.AddInt64(&cntDis, 1)
						continue
					}
					b.Error("Failed to get Message Type from packet proto")
				default:
					b.Error("Failed to parse packet type")
				}
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		agentSocket := makeAgentSocket(proto)
		agentSocket.src = randString(20)
		agentSocket.auth.resp.Stoken = &agentSocket.src
		agentToken := randString(20)
		agentSocket.auth.req.Atoken = &agentToken
		agentSocket.auth.resp.Atoken = &agentToken
		agentSocket.IConnection = fakeSocket
		if !proto.addAgent(agentSocket) {
			b.Fatal("Failed add Agent Socket object")
		}
		for pb.Next() {
			data := &Data{Data: testData}
			if err := moduleSocket.SendDataTo(agentToken, data); err != nil {
				b.Fatal("Failed to send data to fake socket:", err.Error())
			}
			atomic.AddInt64(&cntTotal, 1)
			b.SetBytes(testDataLen)
		}
		if !proto.delAgent(agentSocket) {
			b.Fatal("Failed delete Agent Socket object")
		}
		atomic.AddInt64(&cntAgents, 1)
	})
	b.StopTimer()

	for atomic.LoadInt64(&cntRecv) != atomic.LoadInt64(&cntTotal) {
		runtime.Gosched()
	}
	for atomic.LoadInt64(&cntCon) != atomic.LoadInt64(&cntAgents) {
		runtime.Gosched()
	}
	for atomic.LoadInt64(&cntDis) != atomic.LoadInt64(&cntAgents) {
		runtime.Gosched()
	}

	if fakeSocket.Close() != nil {
		b.Error("Failed close Fake Socket object")
	}

	quit <- struct{}{}
	fakeSocket.SendChan <- nil
	wg.Wait()
}
