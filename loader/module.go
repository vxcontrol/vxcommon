package loader

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/vxcontrol/luar"
	"github.com/vxcontrol/vxcommon/agent"
	"github.com/vxcontrol/vxcommon/lua"
	"github.com/vxcontrol/vxcommon/vxproto"
)

type deferredModuleCallback func() bool

// ModuleState is struct for contains information about module
type ModuleState struct {
	name      string
	item      *ModuleItem
	status    agent.ModuleStatus_Status
	wg        sync.WaitGroup
	luaModule *lua.Module
	luaState  *lua.State
	cbStart   deferredModuleCallback
	cbStop    deferredModuleCallback
}

// NewState is function which constructed ModuleState object
func NewState(mc *ModuleConfig, mi *ModuleItem, p vxproto.IVXProto) (*ModuleState, error) {
	ms := &ModuleState{
		name:   mc.Name,
		item:   mi,
		status: agent.ModuleStatus_UNKNOWN,
	}

	socket := p.NewModule(ms.name, mc.AgentID)
	if socket == nil {
		return nil, errors.New("module socket for " + ms.name + " not initialized")
	}
	ms.cbStart = func() bool {
		agents := make(map[string]*vxproto.AgentInfo)
		for dst, info := range p.GetAgentList() {
			if mc.AgentID == "" || info.ID == mc.AgentID {
				agents[dst] = info
			}
		}
		ms.luaModule.SetAgents(agents)

		if !p.AddModule(socket) {
			socket.Close()
			return false
		}
		return true
	}
	ms.cbStop = func() bool {
		ms.luaModule.SetAgents(make(map[string]*vxproto.AgentInfo))
		return p.DelModule(socket)
	}

	var err error
	if ms.luaState, err = lua.NewState(mi.files); err != nil {
		return nil, errors.New("error with creating new state: " + err.Error())
	}

	if ms.luaModule, err = lua.NewModule(mi.args, ms.luaState, socket); err != nil {
		return nil, errors.New("error with creating new module: " + err.Error())
	}

	// TODO: here need to register API for communication to logging subsystem

	luar.Register(ms.luaState.L, "__config", luar.Map{
		"get_config_schema":        mc.GetConfigSchema,
		"get_default_config":       mc.GetDefaultConfig,
		"get_current_config":       mc.GetCurrentConfig,
		"get_event_data_schema":    mc.GetEventDataSchema,
		"get_event_config_schema":  mc.GetEventConfigSchema,
		"get_default_event_config": mc.GetDefaultEventConfig,
		"get_current_event_config": mc.GetCurrentEventConfig,
		"set_current_config": func(c string) bool {
			if mc.SetCurrentConfig(c) {
				ms.wg.Add(1)
				go func() {
					defer ms.wg.Done()
					ms.luaModule.ControlMsg("update_config", c)
				}()
				return true
			}
			return false
		},
		"get_module_info": func() string {
			if dinfo, err := json.Marshal(mc); err == nil {
				return string(dinfo)
			}
			return ""
		},
		"ctx": luar.Map{
			"agent_id":    mc.AgentID,
			"os":          mc.OS,
			"name":        mc.Name,
			"version":     mc.Version,
			"events":      mc.Events,
			"last_update": mc.LastUpdate,
		},
	})

	ms.status = agent.ModuleStatus_LOADED
	return ms, nil
}

// Start is function for running server module
func (ms *ModuleState) Start() error {
	switch ms.status {
	case agent.ModuleStatus_UNKNOWN:
		return errors.New("module " + ms.name + " can't loaded")
	case agent.ModuleStatus_RUNNING:
		return errors.New("module " + ms.name + " already running")
	case agent.ModuleStatus_FREED:
		return errors.New("module " + ms.name + " already closed")
	case agent.ModuleStatus_LOADED:
		fallthrough
	case agent.ModuleStatus_STOPPED:
		if !ms.cbStart() {
			return errors.New("module socket for " + ms.name + " not registered")
		}
		ms.wg.Add(1)
		ms.status = agent.ModuleStatus_RUNNING
		go func(ms *ModuleState) {
			defer ms.wg.Done()
			ms.luaModule.Start()
			ms.status = agent.ModuleStatus_STOPPED
		}(ms)
	default:
		return errors.New("undefined module " + ms.name + " status")
	}

	return nil
}

// Stop is function for stopping server module
func (ms *ModuleState) Stop() error {
	switch ms.status {
	case agent.ModuleStatus_UNKNOWN:
		return errors.New("module " + ms.name + " can't loaded")
	case agent.ModuleStatus_LOADED:
		ms.status = agent.ModuleStatus_STOPPED
	case agent.ModuleStatus_STOPPED:
		return errors.New("module " + ms.name + " already stopped")
	case agent.ModuleStatus_FREED:
		return errors.New("module " + ms.name + " already closed")
	case agent.ModuleStatus_RUNNING:
		ms.luaModule.Stop()
		if !ms.cbStop() {
			return errors.New("can't stop module socket for " + ms.name)
		}
		ms.status = agent.ModuleStatus_STOPPED
	default:
		return errors.New("undefined module " + ms.name + " status")
	}

	return nil
}

// Close is function for release module object
func (ms *ModuleState) Close() error {
	switch ms.status {
	case agent.ModuleStatus_UNKNOWN:
		return errors.New("module " + ms.name + " can't loaded")
	case agent.ModuleStatus_LOADED:
		return errors.New("module " + ms.name + " wasn't running")
	case agent.ModuleStatus_FREED:
		return errors.New("module " + ms.name + " already closed")
	case agent.ModuleStatus_RUNNING:
		ms.luaModule.Stop()
		if !ms.cbStop() {
			return errors.New("can't stop module socket for " + ms.name)
		}
		ms.status = agent.ModuleStatus_STOPPED
		fallthrough
	case agent.ModuleStatus_STOPPED:
		ms.wg.Wait()
		luar.Register(ms.luaState.L, "__config", luar.Map{})
		ms.luaModule.Close()
		ms.luaState = nil
		ms.status = agent.ModuleStatus_FREED
	default:
		return errors.New("undefined module " + ms.name + " status")
	}

	return nil
}

// GetName is function that return module name
func (ms *ModuleState) GetName() string {
	return ms.name
}

// GetStatus is function that return module status
func (ms *ModuleState) GetStatus() agent.ModuleStatus_Status {
	return ms.status
}

// GetResult is function that return module re3sult after close
func (ms *ModuleState) GetResult() string {
	return ms.luaModule.GetResult()
}

// GetState is function that return current lua state
func (ms *ModuleState) GetState() *lua.State {
	return ms.luaState
}

// GetModule is function that return current lua module object
func (ms *ModuleState) GetModule() *lua.Module {
	return ms.luaModule
}

// GetItem is function that return module item (args, files and config)
func (ms *ModuleState) GetItem() *ModuleItem {
	return ms.item
}
