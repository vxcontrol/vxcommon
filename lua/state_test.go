package lua_test

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/vxcontrol/vxcommon/lua"
)

func init() {
	logrus.SetOutput(ioutil.Discard)
}

// Run simple test with loading state
func TestLoadStateWithMainModule(t *testing.T) {
	files := map[string][]byte{
		"main.lua": []byte(`return 'success'`),
	}

	state, err := lua.NewState(files)
	if err != nil {
		t.Error("Error with creating new state: ", err)
		t.Fail()
	}

	result, err := state.Exec()
	if err != nil {
		t.Error("Error with executing state: ", err)
		t.Fail()
	}
	if result != "success" {
		t.Error("Error with getting result: ", result)
		t.Fail()
	}
}

// Run simple test with loading other module
func TestLoadStateWithOtherModule(t *testing.T) {
	files := map[string][]byte{
		"main.lua":     []byte(`return require('mymodule')`),
		"mymodule.lua": []byte(`return 'success'`),
	}

	state, err := lua.NewState(files)
	if err != nil {
		t.Error("Error with creating new state: ", err)
		t.Fail()
	}

	result, err := state.Exec()
	if err != nil {
		t.Error("Error with executing state: ", err)
		t.Fail()
	}
	if result != "success" {
		t.Error("Error with getting result: ", result)
		t.Fail()
	}
}

func BenchmarkLuaLoadStateWithMainModule(b *testing.B) {
	for i := 0; i < b.N; i++ {
		files := map[string][]byte{
			"main.lua": []byte(`return 'success'`),
		}

		state, err := lua.NewState(files)
		if err != nil {
			b.Fatal("Error with creating new state: ", err)
		}

		result, err := state.Exec()
		if err != nil {
			b.Fatal("Error with executing state: ", err)
		}
		if result != "success" {
			b.Fatal("Error with getting result: ", result)
		}

		// force using GC
		runtime.GC()
	}
}

func BenchmarkLuaLoadStateWithLargeFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		files := map[string][]byte{
			"main.lua":     []byte(`return 'success'`),
			"big_file.dat": make([]byte, 10*1024*1024),
		}

		state, err := lua.NewState(files)
		if err != nil {
			b.Fatal("Error with creating new state: ", err)
		}

		result, err := state.Exec()
		if err != nil {
			b.Fatal("Error with executing state: ", err)
		}
		if result != "success" {
			b.Fatal("Error with getting result: ", result)
		}

		// force using GC
		runtime.GC()
	}
}
