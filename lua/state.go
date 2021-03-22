package lua

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/vxcontrol/golua/lua"
	"github.com/vxcontrol/luar"
)

func moduleLoader(L *lua.State) int {
	moduleName := L.CheckString(1)

	logger := logrus.WithFields(logrus.Fields{
		"component": "state",
		"module":    moduleName,
	})
	var files map[string]string
	L.GetGlobal("__files")
	err := luar.LuaToGo(L, -1, &files)
	L.Pop(1)
	if err != nil {
		logger.WithError(err).Error("failed put module files to the lua state")
	}

	var moduleNames []string
	pathToModule := strings.Replace(moduleName, ".", "/", -1)
	moduleNames = append(moduleNames, pathToModule+"/init.lua")
	moduleNames = append(moduleNames, pathToModule+".lua")
	for _, filePath := range moduleNames {
		moduleData, ok := files[filePath]
		if ok && L.LoadBuffer([]byte(moduleData), len(moduleData), moduleName) != 0 {
			err = fmt.Errorf(L.ToString(-1))
			logger.WithError(err).Error("failed put module data to the lua state")
			L.Pop(1)
			break
		}
	}

	return 1
}

func registerCalls(L *lua.State) {
	L.GetGlobal("unsafe_pcall")
	L.SetGlobal("pcall")
	L.GetGlobal("unsafe_xpcall")
	L.SetGlobal("xpcall")
}

func registerLoader(L *lua.State) {
	top := L.GetTop()
	L.GetGlobal(lua.LUA_LOADLIBNAME)
	L.GetField(-1, "loaders")
	L.PushGoFunction(moduleLoader)
	L.RawSeti(-2, int(L.ObjLen(-2)+1))
	L.SetTop(top)
}

func normPath(args ...string) string {
	var path string

	if runtime.GOOS == "windows" {
		args = append(args, "?.dll")
		path = filepath.Join(args...)
		path = strings.Replace(path, "\\", "\\\\", -1)
	} else if runtime.GOOS == "darwin" {
		path = filepath.Join(append(args, "lib?.dylib")...)
		path = path + ";" + filepath.Join(append(args, "?.dylib")...)
		path = path + ";" + filepath.Join(append(args, "lib?.so")...)
		path = path + ";" + filepath.Join(append(args, "?.so")...)
	} else {
		path = filepath.Join(append(args, "lib?.so")...)
		path = path + ";" + filepath.Join(append(args, "?.so")...)
	}

	return path
}

func registerFFILoader(L *lua.State, tmpdir string) error {
	var err error
	regFFILoader := fmt.Sprintf(`
	local ffi = require'ffi'
	local ffi_load = ffi.load
	--overload ffi.load for received system libs
	function ffi.load(name, ...)
		local libpath = package.searchpath(name, "%s")
		if libpath ~= nil then
			return ffi_load(libpath)
		else
			return ffi_load(name)
		end
	end`, normPath(tmpdir, "sys")+";"+normPath(tmpdir))

	if err = L.DoString(regFFILoader); err != nil {
		return err
	}

	return nil
}

func registerPanicRecover(L *lua.State) {
	currentPanicf := L.AtPanic(nil)
	currentPanicf = L.AtPanic(currentPanicf)
	newPanic := func(L1 *lua.State) int {
		message := L.ToString(-1)
		logrus.WithField("message", message).Error("panic recovery")
		if currentPanicf != nil {
			return currentPanicf(L1)
		}
		return 1
	}
	L.AtPanic(newPanic)
}

// State is context of lua module
type State struct {
	tmpdir string
	closed bool
	L      *lua.State
	logger *logrus.Entry
}

func stateDestructor(s *State) {
	if s.L != nil {
		s.L.Close()
		os.RemoveAll(s.tmpdir)
		s.L = nil
		s.logger.Info("the state was destroyed")
	}
}

// NewState is function which constructed State object
func NewState(files map[string][]byte) (*State, error) {
	if files["main.lua"] == nil {
		return nil, errors.New("main module not found")
	}

	nfiles := make(map[string][]byte)
	for name, data := range files {
		nfiles[name] = data
	}

	var err error
	var tmpdir string
	if tmpdir, err = ioutil.TempDir("", "vxlua-"); err != nil {
		return nil, err
	}

	pathToPID := filepath.Join(tmpdir, "lock.pid")
	pid := strconv.Itoa(os.Getpid())
	if err = ioutil.WriteFile(pathToPID, []byte(pid), 0640); err != nil {
		return nil, err
	}

	s := &State{
		tmpdir: tmpdir,
		closed: true,
		L:      luar.Init(),
		logger: logrus.WithFields(logrus.Fields{
			"component": "state",
			"tmpdir":    filepath.Base(tmpdir),
		}),
	}
	runtime.SetFinalizer(s, stateDestructor)

	registerCalls(s.L)
	registerLoader(s.L)
	registerPanicRecover(s.L)

	if err = s.loadClibs(nfiles); err != nil {
		return nil, err
	}

	if err = s.loadData(nfiles); err != nil {
		return nil, err
	}

	lfiles := make(map[string]string)
	for name, data := range nfiles {
		lfiles[name] = string(data)
	}
	registerFFILoader(s.L, s.tmpdir)
	luar.GoToLua(s.L, tmpdir)
	s.L.SetGlobal("__tmpdir")
	luar.GoToLua(s.L, lfiles)
	s.L.SetGlobal("__files")

	s.logger.Info("the state was created")
	return s, nil
}

func (s *State) loadClibs(files map[string][]byte) error {
	var err error
	clibsPrefix := "clibs/"
	strictPrefix := clibsPrefix + runtime.GOOS + "/" + runtime.GOARCH + "/"

	for name, data := range files {
		if strings.HasPrefix(name, strictPrefix) {
			fname := filepath.Join(s.tmpdir, strings.TrimPrefix(name, strictPrefix))
			fdir := filepath.Dir(fname)
			os.MkdirAll(fdir, os.ModePerm)
			if err = ioutil.WriteFile(fname, data, 0640); err != nil {
				s.logger.WithError(err).WithField("name", fname).Error("failed to write file")
				return err
			}
		}
		if strings.HasPrefix(name, clibsPrefix) {
			delete(files, name)
		}
	}

	loadstr := fmt.Sprintf(`package.cpath = package.cpath .. ";%s"`, normPath(s.tmpdir))
	if err = s.L.DoString(loadstr); err != nil {
		s.logger.WithError(err).Error("failed to add new cpath to the lua state")
		return err
	}

	s.logger.Debug("the state loaded clibs")
	return nil
}

func (s *State) loadData(files map[string][]byte) error {
	var err error
	prefix := "data/"

	for name, data := range files {
		if strings.HasPrefix(name, prefix) {
			fname := filepath.Join(s.tmpdir, name)
			fdir := filepath.Dir(fname)
			os.MkdirAll(fdir, os.ModePerm)
			if err = ioutil.WriteFile(fname, data, 0640); err != nil {
				s.logger.WithError(err).WithField("name", fname).Error("failed to write file")
				return err
			}
			delete(files, name)
		}
	}

	s.logger.Debug("the state loaded data")
	return nil
}

// Exec is blocked function for data execution
func (s *State) Exec() (string, error) {
	s.closed = false
	defer func() {
		s.closed = true
	}()

	s.logger.Info("the state was started")
	defer s.logger.Info("the state was stopped")

	err := s.L.DoString(`return require('main')`)
	if err != nil {
		s.logger.WithError(err).Error("the state executing catched error")
		return "", err
	}

	return s.L.CheckString(1), nil
}

// IsClose is nonblocked function which check a state of module
func (s *State) IsClose() bool {
	return s.closed
}
