package lua

import (
	"errors"
	"sync"

	"github.com/vxcontrol/golua/lua"
	"github.com/vxcontrol/luar"
)

type luaCallback struct {
	refTr  int
	refCb  int
	l      *lua.State
	mx     *sync.Mutex
	closed bool
}

func newLuaCallback(L *lua.State) *luaCallback {
	if !L.IsFunction(-1) {
		if !L.GetMetaField(-1, "__call") {
			L.Pop(1)
			return nil
		}
		// There leave the __call metamethod on stack.
		L.Remove(-2)
	}

	l := L.NewThread()
	refTr := L.Ref(lua.LUA_REGISTRYINDEX)
	refCb := L.Ref(lua.LUA_REGISTRYINDEX)

	return &luaCallback{l: l, refCb: refCb, refTr: refTr, mx: &sync.Mutex{}}
}

func (lc *luaCallback) Call(results interface{}, args ...interface{}) error {
	lc.mx.Lock()
	defer lc.mx.Unlock()

	if lc.closed {
		return errors.New("callback has already closed")
	}

	// Push the callable value.
	lc.l.RawGeti(lua.LUA_REGISTRYINDEX, lc.refCb)

	// Push the args.
	for _, arg := range args {
		luar.GoToLuaProxy(lc.l, arg)
	}

	if err := lc.l.Call(len(args), 1); err != nil {
		lc.l.Pop(1)
		return err
	}

	if err := luar.LuaToGo(lc.l, -1, results); err != nil {
	}
	lc.l.Pop(1)

	return nil
}

func (lc *luaCallback) Close() {
	lc.mx.Lock()
	defer lc.mx.Unlock()

	if !lc.closed {
		lc.l.Unref(lua.LUA_REGISTRYINDEX, lc.refCb)
		lc.l.Unref(lua.LUA_REGISTRYINDEX, lc.refTr)
		lc.closed = true
	}
}
