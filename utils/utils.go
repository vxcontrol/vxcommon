package utils

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/vxcontrol/vxcommon/agent"
	"github.com/vxcontrol/vxcommon/vxproto"
)

// InList is function for searching in array of string
func InList(list []string, str string) bool {
	for _, item := range list {
		if item == str {
			return true
		}
	}
	return false
}

// GetRef is function for returning referense of string
func GetRef(str string) *string {
	return &str
}

// RemoveUnusedTempDir is public function that should use into worker on start
func RemoveUnusedTempDir() {
	files, err := ioutil.ReadDir(os.TempDir())
	if err != nil {
		return
	}

	for _, f := range files {
		if f.IsDir() && strings.HasPrefix(f.Name(), "vxlua-") {
			pathToPID := filepath.Join(os.TempDir(), f.Name(), "lock.pid")
			fdata, err := ioutil.ReadFile(pathToPID)
			if err != nil {
				os.RemoveAll(filepath.Join(os.TempDir(), f.Name()))
				continue
			}
			pid, err := strconv.Atoi(string(fdata))
			if err != nil {
				os.RemoveAll(filepath.Join(os.TempDir(), f.Name()))
				continue
			}
			proc, _ := os.FindProcess(pid)
			if proc == nil || err != nil {
				os.RemoveAll(filepath.Join(os.TempDir(), f.Name()))
				continue
			}
		}
	}
}

// HasInfoValid is function which validate information from agent
func HasInfoValid(info *agent.Information) bool {
	switch info.GetOs().GetType() {
	case "linux":
	case "windows":
	case "darwin":
	default:
		return false
	}

	switch info.GetOs().GetArch() {
	case "amd64":
	case "386":
	default:
		return false
	}

	return true
}

// GetAgentInformation is additional function for handshake logic
func GetAgentInformation() *agent.Information {
	infoMessage := &agent.Information{
		Os: &agent.Information_OS{
			Type: GetRef(runtime.GOOS),
			Name: GetRef("unknown"),
			Arch: GetRef(runtime.GOARCH),
		},
		User: &agent.Information_User{
			Name:  GetRef("unknown"),
			Group: GetRef("unknown"),
		},
	}

	return infoMessage
}

// DoHandshakeWithServerOnAgent is function which implemented agent logic of handshake
func DoHandshakeWithServerOnAgent(socket vxproto.IAgentSocket) error {
	if !socket.HasAgentIDValid(socket.GetPublicInfo().ID, vxproto.VXAgent) {
		return errors.New("agent id was corrupted")
	}

	seconds := time.Now().Unix()
	authReqMessage := &agent.AuthenticationRequest{
		Timestamp: &seconds,
		Atoken:    GetRef(socket.GetSource()),
	}
	authMessageData, err := proto.Marshal(authReqMessage)
	if err != nil {
		return err
	}
	socket.SetAuthReq(authReqMessage)

	if err = socket.Write(authMessageData); err != nil {
		return err
	}

	var authRespMessage agent.AuthenticationResponse
	authMessageData, err = socket.Read()
	if err = proto.Unmarshal(authMessageData, &authRespMessage); err != nil {
		return err
	}
	if authRespMessage.GetAtoken() == "" && authRespMessage.GetStoken() == "" {
		return errors.New("failed auth on server side")
	}
	if !socket.HasTokenCRCValid(authRespMessage.GetAtoken()) {
		return errors.New("agent token was corrupted")
	}
	if !socket.HasTokenCRCValid(authRespMessage.GetStoken()) {
		return errors.New("server token was corrupted")
	}
	socket.SetAuthResp(&authRespMessage)
	socket.SetSource(authRespMessage.GetAtoken())

	infoMessage := GetAgentInformation()
	if !socket.HasAgentInfoValid(socket.GetPublicInfo().ID, infoMessage) {
		return errors.New("agent info was corrupted")
	}

	infoMessageData, err := proto.Marshal(infoMessage)
	if err != nil {
		return err
	}
	if err = socket.Write(infoMessageData); err != nil {
		return err
	}
	socket.SetInfo(infoMessage)

	return nil
}

// DoHandshakeWithAgentOnServer is function which implemented server logic of handshake
func DoHandshakeWithAgentOnServer(socket vxproto.IAgentSocket) error {
	if !socket.HasAgentIDValid(socket.GetPublicInfo().ID, vxproto.VXAgent) {
		failedRespMessage := &agent.AuthenticationResponse{
			Atoken: GetRef(""),
			Stoken: GetRef(""),
		}
		failedMessageData, err := proto.Marshal(failedRespMessage)
		if err != nil {
			return err
		}

		if err = socket.Write(failedMessageData); err != nil {
			return err
		}

		return errors.New("agent state is busy or id was corrupted")
	}

	authMessageData, err := socket.Read()
	var authReqMessage agent.AuthenticationRequest
	err = proto.Unmarshal(authMessageData, &authReqMessage)
	if err != nil {
		return err
	}

	socket.SetAuthReq(&authReqMessage)
	token := authReqMessage.GetAtoken()
	if !socket.HasTokenValid(token) {
		token = socket.NewToken()
	}
	authRespMessage := &agent.AuthenticationResponse{
		Atoken: &token,
		Stoken: GetRef(socket.GetSource()),
	}
	authMessageData, err = proto.Marshal(authRespMessage)
	if err != nil {
		return err
	}
	socket.SetAuthResp(authRespMessage)

	if err = socket.Write(authMessageData); err != nil {
		return err
	}

	var infoMessage agent.Information
	infoMessageData, err := socket.Read()
	if err = proto.Unmarshal(infoMessageData, &infoMessage); err != nil {
		return err
	}

	if !socket.HasAgentInfoValid(socket.GetPublicInfo().ID, &infoMessage) {
		return errors.New("agent info was corrupted")
	}
	socket.SetInfo(&infoMessage)

	return nil
}

// DoHandshakeWithServerOnBrowser is function which implemented agent logic of handshake
// It implemented for testing only
func DoHandshakeWithServerOnBrowser(socket vxproto.IAgentSocket) error {
	if !socket.HasAgentIDValid(socket.GetPublicInfo().ID, vxproto.Browser) {
		return errors.New("agent id was corrupted")
	}

	seconds := time.Now().Unix()
	authReqMessage := &agent.AuthenticationRequest{
		Timestamp: &seconds,
		Atoken:    GetRef(socket.GetSource()),
	}
	authMessageData, err := proto.Marshal(authReqMessage)
	if err != nil {
		return err
	}
	socket.SetAuthReq(authReqMessage)

	if err = socket.Write(authMessageData); err != nil {
		return err
	}

	var authRespMessage agent.AuthenticationResponse
	authMessageData, err = socket.Read()
	if err = proto.Unmarshal(authMessageData, &authRespMessage); err != nil {
		return err
	}
	if authRespMessage.GetAtoken() == "" && authRespMessage.GetStoken() == "" {
		return errors.New("failed auth on server side")
	}
	if !socket.HasTokenCRCValid(authRespMessage.GetAtoken()) {
		return errors.New("browser token was corrupted")
	}
	if !socket.HasTokenCRCValid(authRespMessage.GetStoken()) {
		return errors.New("server token was corrupted")
	}
	socket.SetAuthResp(&authRespMessage)
	socket.SetSource(authRespMessage.GetAtoken())

	return nil
}

// DoHandshakeWithBrowserOnServer is function which implemented server logic of handshake
func DoHandshakeWithBrowserOnServer(socket vxproto.IAgentSocket) error {
	if !socket.HasAgentIDValid(socket.GetPublicInfo().ID, vxproto.Browser) {
		failedRespMessage := &agent.AuthenticationResponse{
			Atoken: GetRef(""),
			Stoken: GetRef(""),
		}
		failedMessageData, err := proto.Marshal(failedRespMessage)
		if err != nil {
			return err
		}

		if err = socket.Write(failedMessageData); err != nil {
			return err
		}

		return errors.New("agent state is busy or id was corrupted")
	}

	authMessageData, err := socket.Read()
	var authReqMessage agent.AuthenticationRequest
	err = proto.Unmarshal(authMessageData, &authReqMessage)
	if err != nil {
		return err
	}

	socket.SetAuthReq(&authReqMessage)
	token := authReqMessage.GetAtoken()
	if !socket.HasTokenValid(token) {
		token = socket.NewToken()
	}
	authRespMessage := &agent.AuthenticationResponse{
		Atoken: &token,
		Stoken: GetRef(socket.GetSource()),
	}
	authMessageData, err = proto.Marshal(authRespMessage)
	if err != nil {
		return err
	}
	socket.SetAuthResp(authRespMessage)

	if err = socket.Write(authMessageData); err != nil {
		return err
	}

	return nil
}
