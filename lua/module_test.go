package lua_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vxcontrol/luar"
	"github.com/vxcontrol/vxcommon/agent"
	"github.com/vxcontrol/vxcommon/lua"
	"github.com/vxcontrol/vxcommon/vxproto"
)

func randString(nchars int) string {
	rbytes := make([]byte, nchars)
	if _, err := rand.Read(rbytes); err != nil {
		return ""
	}

	return hex.EncodeToString(rbytes)
}

// getRef is function for returning referense of string
func getRef(str string) *string {
	return &str
}

type FakeMainModule struct{}

func (mModule *FakeMainModule) DefaultRecvPacket(packet *vxproto.Packet) error {
	return nil
}

func (mModule *FakeMainModule) HasAgentIDValid(agentID string, agentType vxproto.AgentType) bool {
	return true
}

func (mModule *FakeMainModule) HasAgentInfoValid(agentID string, info *agent.Information) bool {
	return true
}

func (mModule *FakeMainModule) OnConnect(socket vxproto.IAgentSocket) error {
	seconds := time.Now().Unix()
	stoken := randString(20)
	socket.SetAuthReq(&agent.AuthenticationRequest{
		Timestamp: &seconds,
		Atoken:    getRef(""),
	})
	socket.SetAuthResp(&agent.AuthenticationResponse{
		Atoken: getRef(socket.GetSource()),
		Stoken: getRef(stoken),
	})
	socket.SetInfo(&agent.Information{
		Os: &agent.Information_OS{
			Type: getRef("linux"),
			Name: getRef("Ubuntu 16.04"),
			Arch: getRef("amd64"),
		},
		User: &agent.Information_User{
			Name:  getRef("root"),
			Group: getRef("root"),
		},
	})
	return nil
}

type MFiles map[string][]byte
type MArgs map[string][]string

func init() {
	logrus.SetOutput(ioutil.Discard)
}

// Init module
func initModule(files MFiles, args MArgs, name string, proto vxproto.IVXProto) (*lua.Module, *lua.State) {
	if proto == nil {
		return nil, nil
	}

	moduleSocket := proto.NewModule(name, "")
	if moduleSocket == nil {
		return nil, nil
	}

	if !proto.AddModule(moduleSocket) {
		return nil, nil
	}

	state, err := lua.NewState(files)
	if err != nil {
		return nil, nil
	}

	module, err := lua.NewModule(args, state, moduleSocket)
	if err != nil {
		return nil, nil
	}

	luar.Register(state.L, "", luar.Map{
		"print": fmt.Println,
	})

	return module, state
}

// Run main logic of module
func runModule(module *lua.Module) string {
	if module == nil {
		return "internal error"
	}
	module.Start()
	return module.GetResult()
}

// Run simple test with arguments for state
func Example_args() {
	arg1 := []string{"a", "b", "c", "d"}
	arg2 := []string{"e"}
	arg3 := []string{}
	arg4 := []string{"f", "g"}
	args := map[string][]string{
		"arg1": arg1,
		"arg2": arg2,
		"arg3": arg3,
		"arg4": arg4,
	}
	files := map[string][]byte{
		"main.lua": []byte(`
			local arg_keys = {}
			for k, _ in pairs(__args) do
				table.insert(arg_keys, k)
			end
			table.sort(arg_keys)
			for i, k in ipairs(arg_keys) do
				v = __args[k]
				print(i, k, table.getn(v))
				for j, p in pairs(v) do
					print(j, p)
				end
			end
			return "success"
		`),
	}

	proto := vxproto.New(&FakeMainModule{})
	module, _ := initModule(files, args, "test_module", proto)
	fmt.Println("Result: ", runModule(module))
	module.Close()
	proto.Close()
	// Output:
	//1 arg1 4
	//1 a
	//2 b
	//3 c
	//4 d
	//2 arg2 1
	//1 e
	//3 arg3 0
	//4 arg4 2
	//1 f
	//2 g
	//Result:  success
}

// Run simple test with await function (1 second)
func Example_module_await() {
	args := map[string][]string{}
	files := map[string][]byte{
		"main.lua": []byte(`
			print(__api.get_name())
			__api.await(1000) -- 1 sec
			return "success"
		`),
	}

	proto := vxproto.New(&FakeMainModule{})
	module, _ := initModule(files, args, "test_module", proto)
	fmt.Println("Result: ", runModule(module))
	module.Close()
	proto.Close()
	// Output:
	//test_module
	//Result:  success
}

// Run simple test with await function (infinity)
func Example_module_await_inf() {
	args := map[string][]string{}
	files := map[string][]byte{
		"main.lua": []byte(`
			print(__api.get_name())
			__api.await(-1)
			return "success"
		`),
	}

	var wg sync.WaitGroup
	proto := vxproto.New(&FakeMainModule{})
	module, _ := initModule(files, args, "test_module", proto)
	wg.Add(1)
	go func() {
		fmt.Println("Result: ", runModule(module))
		wg.Done()
	}()

	time.Sleep(time.Second)
	module.Close()
	proto.Close()
	wg.Wait()
	// Output:
	//test_module
	//Result:  success
}

// Run simple test with all API functions
func Example_api() {
	args := map[string][]string{}
	files := map[string][]byte{
		"main.lua": []byte(`
			__api.set_recv_timeout(10) -- 10 ms

			-- src is string
			-- data is string
			-- path is string
			-- name is string
			-- mtype is int:
			--   {DEBUG: 0, INFO: 1, WARNING: 2, ERROR: 3}
			-- cmtype is string
			-- data is string
			if __api.add_cbs({
				["data"] = function(src, data)
					return true
				end,
				["file"] = function(src, path, name)
					return true
				end,
				["text"] = function(src, text, name)
					return true
				end,
				["msg"] = function(src, msg, mtype)
					return true
				end,
				["control"] = function(cmtype, data)
					return true
				end,
			}) == false then
				return "failed"
			end
			if __api.del_cbs({ "data", "file", "text", "msg", "control" }) == false then
				return "failed"
			end

			-- res is boolean
			res = __api.send_data_to("def_token", "test_data")
			res = __api.send_file_to("def_token", "file_data", "file_name")
			res = __api.send_text_to("def_token", "text_data", "text_name")
			res = __api.send_msg_to("def_token", "msg_data", 0)
			res = __api.send_file_from_fs_to("def_token", "file_path", "file_name")

			-- res is boolean
			src, data, res = __api.recv_data()
			src, path, name, res = __api.recv_file()
			src, text, name, res = __api.recv_text()
			src, msg, mtype, res = __api.recv_msg()

			-- res is boolean
			data, res = __api.recv_data_from("def_token")
			path, name, res = __api.recv_file_from("def_token")
			text, name, res = __api.recv_text_from("def_token")
			msg, mtype, res = __api.recv_msg_from("def_token")

			return "success"
		`),
	}

	proto := vxproto.New(&FakeMainModule{})
	module, _ := initModule(files, args, "test_module", proto)
	fmt.Println("Result: ", runModule(module))
	module.Close()
	proto.Close()
	// Output:
	//Result:  success
}

// Run IMC test with the API functions
func Example_imc() {
	var wg sync.WaitGroup
	mname1 := []string{"test_module1", "sender"}
	mname2 := []string{"test_module2", "receiver"}
	args1 := map[string][]string{
		"dst_module": mname2,
	}
	args2 := map[string][]string{
		"dst_module": mname1,
	}
	files := map[string][]byte{
		"main.lua": []byte(`
			local imc_token = __imc.make_token("", __args["dst_module"][1])
			__api.set_recv_timeout(1000)

			local this_imc_token = __imc.get_token()
			if type(this_imc_token) ~= "string" or #this_imc_token ~= 40 then
				return "failed"
			end
			if __imc.is_exist(imc_token) == false then
				return "failed"
			end
			local agentID, modName, isExist = __imc.get_info(imc_token)
			if agentID ~= "" or modName ~= __args["dst_module"][1] or isExist ~= true then
				return "failed"
			end

			if __args["dst_module"][2] == "sender" then
				__api.await(100)
				if __api.send_data_to(imc_token, "test_data") == false then
					return "failed"
				end
				
				__api.await(100)
				if __api.send_data_to(imc_token, "test_data_direct") == false then
					return "failed"
				end
			end

			if __args["dst_module"][2] == "receiver" then
				local src, data, res = __api.recv_data()
				if src ~= imc_token or data ~= "test_data" or res ~= true then
					return "failed"
				end
	
				data, res = __api.recv_data_from(imc_token)
				if data ~= "test_data_direct" or res ~= true then
					return "failed"
				end
			end

			return "success"
		`),
	}

	proto := vxproto.New(&FakeMainModule{})
	module1, _ := initModule(files, args1, mname1[0], proto)
	module2, _ := initModule(files, args2, mname2[0], proto)

	wg.Add(1)
	go func() {
		fmt.Println("Result1: ", runModule(module1))
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		fmt.Println("Result2: ", runModule(module2))
		wg.Done()
	}()
	wg.Wait()
	module1.Close()
	module2.Close()
	proto.Close()
	// Unordered output:
	//Result1:  success
	//Result2:  success
}

// Run simple test to start of stopped module
func TestLuaStartStoppedModule(t *testing.T) {
	args := map[string][]string{}
	files := map[string][]byte{
		"main.lua": []byte(`return 'success'`),
	}

	proto := vxproto.New(&FakeMainModule{})
	module, _ := initModule(files, args, "test_module", proto)
	if runModule(module) != "success" {
		t.Fatal("Error on getting result from module (step 1)")
	}
	module.Stop()
	if runModule(module) != "success" {
		t.Fatal("Error on getting result from module (step 2)")
	}
	module.Close()
	if err := proto.Close(); err != nil {
		t.Fatal("Error on close vxproto object: ", err.Error())
	}
}

func BenchmarkLuaLoadModuleWithMainModule(b *testing.B) {
	proto := vxproto.New(&FakeMainModule{})
	serveModule := func() {
		name := "test"
		args := map[string][]string{}
		files := map[string][]byte{
			"main.lua": []byte(`return 'success'`),
		}

		socket := proto.NewModule(name, "")
		if socket == nil {
			b.Fatal("Error with making new module in vxproto")
		}

		if !proto.AddModule(socket) {
			b.Fatal("Error with adding new module into vxproto")
		}

		state, err := lua.NewState(files)
		if err != nil {
			b.Fatal("Error with making new lua state: ", err.Error())
		}

		module, err := lua.NewModule(args, state, socket)
		if err != nil {
			b.Fatal("Error with making new lua module: ", err.Error())
		}

		module.Start()
		result := module.GetResult()
		if result != "success" {
			b.Fatal("Error with getting result: ", result)
		}

		module.Stop()
		if !proto.DelModule(socket) {
			b.Fatal("Error with deleting module from vxproto")
		}

		module.Close()
	}

	for i := 0; i < b.N; i++ {
		serveModule()
		// force using GC
		runtime.GC()
	}

	if err := proto.Close(); err != nil {
		b.Fatal("Failed to close vxproto object: ", err.Error())
	}
}

func BenchmarkLuaLinkAgentToModule(b *testing.B) {
	var cntAccepted int64
	var wg sync.WaitGroup
	limitConnections := 500
	args := map[string][]string{}
	files := map[string][]byte{
		"main.lua": []byte(`
			local con, dis = 0, 0
			__api.set_recv_timeout(10)
			if __api.add_cbs({
				["data"] = function(src, data)
					return true
				end,
				["control"] = function(cmtype, data)
					if cmtype == "agent_connected" then
						con = con + 1
					end
					if cmtype == "agent_disconnected" then
						dis = dis + 1
					end
					return true
				end,
			}) == false then
				return "failed"
			end

			__api.await(-1)
			if con == dis and con ~= 0 then
				return tostring(con)
			else
				return "failed"
			end
		`),
	}

	proto_agent := vxproto.New(&FakeMainModule{})
	if proto_agent == nil {
		b.Fatal("Failed initialize VXProto object for agent")
	}
	proto_server := vxproto.New(&FakeMainModule{})
	if proto_server == nil {
		b.Fatal("Failed initialize VXProto object for server")
	}
	module, _ := initModule(files, args, "test", proto_server)

	wg.Add(1)
	go func() {
		defer wg.Done()
		cfg := map[string]string{
			"listen": "ws://0.0.0.0:28080",
			"scheme": "ws",
		}
		if err := proto_server.Listen(cfg); err != nil {
			if !strings.HasSuffix(err.Error(), "Server closed") {
				b.Error("Failed to listen socket", err.Error())
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		result := runModule(module)
		if result == "failed" {
			b.Error("Failed to process agent connections on the lua module")
		}
		if result_conns, err := strconv.Atoi(result); err != nil {
			b.Error("Failed to result from the lua module")
		} else {
			cntAccepted += int64(result_conns)
		}
	}()

	time.Sleep(2 * time.Second)
	b.ResetTimer()
	var cnt, cntRequested int64
	var wgc sync.WaitGroup
	for i := 0; i < b.N; i++ {
		atomic.AddInt64(&cnt, 1)
		wgc.Add(1)
		go func() {
			defer wgc.Done()
			cfg := map[string]string{
				"connection": "ws://localhost:28080",
				"id":         randString(16),
				"token":      randString(20),
			}
			if err := proto_agent.Connect(cfg); err != nil {
				// TODO: here need to separate connection errors
				err1 := strings.HasSuffix(err.Error(), "use of closed network connection")
				err2 := strings.HasSuffix(err.Error(), "connection reset by peer")
				err3 := strings.HasSuffix(err.Error(), "broken pipe")
				err4 := strings.HasSuffix(err.Error(), "protocol wrong type for socket")
				if !err1 && !err2 && !err3 && !err4 {
					b.Error("Failed to connect to module socket", err.Error())
				}
				atomic.AddInt64(&cnt, -1)
			}
		}()
		if int(atomic.LoadInt64(&cnt)) >= limitConnections {
			for int(atomic.LoadInt64(&cnt)) != proto_agent.GetAgentsCount() {
				runtime.Gosched()
			}
			cntRequested += atomic.LoadInt64(&cnt)
			proto_agent.Close()
			wgc.Wait()
			cnt = 0
		}
	}
	if int(atomic.LoadInt64(&cnt)) != 0 {
		for int(atomic.LoadInt64(&cnt)) != len(proto_agent.GetAgentList()) {
			runtime.Gosched()
		}
		cntRequested += atomic.LoadInt64(&cnt)
		proto_agent.Close()
		wgc.Wait()
		cnt = 0
	}
	b.StopTimer()

	time.Sleep(time.Second)
	module.Close()
	if err := proto_server.Close(); err != nil {
		b.Fatal("Failed to close vxproto object: ", err.Error())
	}
	wg.Wait()

	if cntRequested != cntAccepted {
		b.Fatal("Mismatch amount connections | requested: ", cntRequested, " | accepted: ", cntAccepted)
	}
}
