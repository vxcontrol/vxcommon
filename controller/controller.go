package controller

import (
	"errors"
	"sync"
	"time"

	"github.com/vxcontrol/vxcommon/loader"
	"github.com/vxcontrol/vxcommon/lua"
	"github.com/vxcontrol/vxcommon/vxproto"
)

// IController is interface for loading and control all modules
type IController interface {
	Load() error
	Close() error
	Lock()
	Unlock()
	GetModuleState(id string) *loader.ModuleState
	GetModuleStates(ids []string) map[string]*loader.ModuleState
	GetModule(id string) *Module
	GetModules(ids []string) map[string]*Module
	GetModuleIds() []string
	GetSharedModuleIds() []string
	GetModuleIdsForAgent(agentID string) []string
	StartAllModules() ([]string, error)
	StartSharedModules() ([]string, error)
	StartModulesForAgent(agentID string) ([]string, error)
	StopSharedModules() ([]string, error)
	StopModulesForAgent(agentID string) ([]string, error)
	StopAllModules() ([]string, error)
	SetUpdateChan(update chan struct{})
	SetUpdateChanForAgent(agentID string, update chan struct{})
	UnsetUpdateChan(update chan struct{})
	UnsetUpdateChanForAgent(agentID string, update chan struct{})
}

// IRegAPI is interface of Main Module for registrate extra Lua API
type IRegAPI interface {
	RegisterLuaAPI(L *lua.State, config *loader.ModuleConfig) error
	UnregisterLuaAPI(L *lua.State, config *loader.ModuleConfig) error
}

// sController is universal container for modules
type sController struct {
	regAPI  IRegAPI
	config  IConfigLoader
	files   IFilesLoader
	loader  loader.ILoader
	modules []*Module
	mutex   *sync.Mutex
	proto   vxproto.IVXProto
	wg      sync.WaitGroup
	quit    chan struct{}
	updates map[string][]chan struct{}
	closed  bool
}

// Module is struct for contains module struct from store
type Module struct {
	id     string
	config *loader.ModuleConfig
	files  *loader.ModuleFiles
}

// GetID is function that return module id
func (m *Module) GetID() string {
	return m.id
}

// GetConfig is function that return module config object
func (m *Module) GetConfig() *loader.ModuleConfig {
	return m.config
}

// GetFiles is function that return module files object
func (m *Module) GetFiles() *loader.ModuleFiles {
	return m.files
}

// NewController is function which constructed Controller object
func NewController(r IRegAPI, c IConfigLoader, f IFilesLoader, p vxproto.IVXProto) IController {
	return &sController{
		regAPI:  r,
		config:  c,
		files:   f,
		proto:   p,
		mutex:   &sync.Mutex{},
		loader:  loader.New(),
		quit:    make(chan struct{}),
		updates: make(map[string][]chan struct{}),
	}
}

// Lock is unsafe function that locked controller state
func (s *sController) Lock() {
	s.mutex.Lock()
}

// Unlock is unsafe function that unlocked controller state
func (s *sController) Unlock() {
	s.mutex.Unlock()
}

// Load is function that retrieve modules list
func (s *sController) Load() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.config == nil {
		return errors.New("config loader object not found")
	}
	if s.files == nil {
		return errors.New("files loader object not found")
	}

	config, err := s.config.load()
	if err != nil {
		return errors.New("load module configuration failed: " + err.Error())
	}

	files, err := s.files.load(config)
	if err != nil {
		return errors.New("load module files failed: " + err.Error())
	}
	if len(config) != len(files) {
		return errors.New("failed loading modules files by config")
	}

	s.modules = make([]*Module, len(config), (cap(config)+1)*2)
	for idx, mc := range config {
		s.modules[idx] = s.newModule(mc, files[idx])
	}

	s.wg.Add(1)
	go s.updaterModules()

	return nil
}

// Close is function that stop update watcher and all modules
func (s *sController) Close() error {
	if !s.closed {
		s.quit <- struct{}{}
		s.wg.Wait()
	}

	_, err := s.StopAllModules()

	return err
}

// GetModuleState is function that return a module state by id
func (s *sController) GetModuleState(id string) *loader.ModuleState {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.loader.Get(id)
}

// GetModuleStates is function that return module states by id list
func (s *sController) GetModuleStates(ids []string) map[string]*loader.ModuleState {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	states := make(map[string]*loader.ModuleState)
	for _, id := range ids {
		states[id] = s.loader.Get(id)
	}

	return states
}

// GetModule is function that return a module object by id
func (s *sController) GetModule(id string) *Module {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for idx := range s.modules {
		module := s.modules[idx]
		if module.id == id {
			return module
		}
	}

	return nil
}

// GetModules is function that return module objects by id list
func (s *sController) GetModules(ids []string) map[string]*Module {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	modules := make(map[string]*Module)
	for idx := range s.modules {
		module := s.modules[idx]
		for _, id := range ids {
			if module.id == id {
				modules[id] = module
				break
			}
		}
	}

	return modules
}

// GetModuleIds is function that return module id list
func (s *sController) GetModuleIds() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var ids []string
	for idx := range s.modules {
		ids = append(ids, s.modules[idx].id)
	}

	return ids
}

// GetSharedModuleIds is function that return shared module id list
func (s *sController) GetSharedModuleIds() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var ids []string
	for idx := range s.modules {
		module := s.modules[idx]
		if module.config.AgentID == "" {
			ids = append(ids, module.id)
		}
	}

	return ids
}

// GetModuleIdsForAgent is function that return module id list for agent id
func (s *sController) GetModuleIdsForAgent(agentID string) []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var ids []string
	for idx := range s.modules {
		module := s.modules[idx]
		if module.config.AgentID == agentID {
			ids = append(ids, module.id)
		}
	}

	return ids
}

// StartAllModules is function for start all modules
func (s *sController) StartAllModules() ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	defer s.notifyModules("")

	var ids []string
	for idx := range s.modules {
		module := s.modules[idx]
		if err := s.startModule(module); err != nil {
			return nil, err
		}
		ids = append(ids, module.id)
	}

	return ids, nil
}

// StartSharedModules is function for start shared modules
func (s *sController) StartSharedModules() ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	defer s.notifyModules("")

	var ids []string
	for idx := range s.modules {
		module := s.modules[idx]
		if module.config.AgentID == "" {
			if err := s.startModule(module); err != nil {
				return nil, err
			}
		}
		ids = append(ids, module.id)
	}

	return ids, nil
}

// StartModulesForAgent is function for start of an agent modules
func (s *sController) StartModulesForAgent(agentID string) ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	defer s.notifyModules(agentID)

	var ids []string
	for idx := range s.modules {
		module := s.modules[idx]
		if module.config.AgentID == agentID {
			if err := s.startModule(module); err != nil {
				return nil, err
			}
		}
		ids = append(ids, module.id)
	}

	return ids, nil
}

// StopSharedModules is function for stop shared modules
func (s *sController) StopSharedModules() ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	defer s.notifyModules("")

	var ids []string
	for idx := range s.modules {
		module := s.modules[idx]
		if module.config.AgentID == "" {
			if err := s.stopModule(module); err != nil {
				return nil, err
			}
		}
		ids = append(ids, module.id)
	}

	return ids, nil
}

// StopModulesForAgent is function for stop of an agent modules
func (s *sController) StopModulesForAgent(agentID string) ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	defer s.notifyModules(agentID)

	var ids []string
	for mdx := range s.modules {
		module := s.modules[mdx]
		if module.config.AgentID == agentID {
			if err := s.stopModule(module); err != nil {
				return nil, err
			}
		}
		ids = append(ids, module.id)
	}

	return ids, nil
}

// StopAllModules is function for stop all modules
func (s *sController) StopAllModules() ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	defer s.notifyModules("")

	var ids []string
	for idx := range s.modules {
		module := s.modules[idx]
		if err := s.stopModule(module); err != nil {
			return nil, err
		}
		ids = append(ids, module.id)
	}

	return ids, nil
}

func (s *sController) SetUpdateChan(update chan struct{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.updates[""] = append(s.updates[""], update)
}

func (s *sController) SetUpdateChanForAgent(agentID string, update chan struct{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.updates[agentID] = append(s.updates[agentID], update)
}

func (s *sController) UnsetUpdateChan(update chan struct{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for idx, u := range s.updates[""] {
		if u == update {
			// It's possible because there used return in the bottom
			s.updates[""] = append(s.updates[""][:idx], s.updates[""][idx+1:]...)
			break
		}
	}

	if len(s.updates[""]) == 0 {
		delete(s.updates, "")
	}
}

func (s *sController) UnsetUpdateChanForAgent(agentID string, update chan struct{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for idx, u := range s.updates[agentID] {
		if u == update {
			// It's possible because there used return in the bottom
			s.updates[agentID] = append(s.updates[agentID][:idx], s.updates[agentID][idx+1:]...)
			break
		}
	}

	if len(s.updates[agentID]) == 0 {
		delete(s.updates, agentID)
	}
}

func (s *sController) newModule(mc *loader.ModuleConfig, mf *loader.ModuleFiles) *Module {
	if ci, ok := mc.IConfigItem.(*sConfigItem); ok {
		setcb := ci.setCurrentConfig
		agentID := mc.AgentID
		ci.setCurrentConfig = func(c string) bool {
			res := setcb(c)
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.mutex.Lock()
				defer s.mutex.Unlock()

				if res {
					s.notifyModules(agentID)
				}
			}()
			return res
		}
	} else {
		// TODO: here need implement base method for redefine SetCurConfig function
	}

	return &Module{
		id:     mc.AgentID + ":" + mc.Name,
		config: mc,
		files:  mf,
	}
}

// startModule is internal function for start a module
func (s *sController) startModule(module *Module) error {
	name := module.config.Name
	if s.loader.Get(module.id) == nil {
		state, err := loader.NewState(module.config, module.files.GetSModule(), s.proto)
		if err != nil {
			return errors.New("failed to initialize " + name + " Module State: " + err.Error())
		}

		if !s.loader.Add(module.id, state) {
			return errors.New("failed to add " + name + " Module State to loader")
		}

		if err = s.regAPI.RegisterLuaAPI(state.GetState(), module.config); err != nil {
			return errors.New("failed to register extra API for " + name + ": " + err.Error())
		}
	}

	return s.loader.Start(module.id)
}

// stopModule is internal function for stop a module
func (s *sController) stopModule(module *Module) error {
	var state *loader.ModuleState
	name := module.config.Name
	if state = s.loader.Get(module.id); state == nil {
		return errors.New(name + " module State in loader not found")
	}

	if err := s.loader.Stop(module.id); err != nil {
		return errors.New("failed to stop " + name + " Module State: " + err.Error())
	}

	if err := s.regAPI.UnregisterLuaAPI(state.GetState(), module.config); err != nil {
		return errors.New("failed to unregister extra API for " + name + ": " + err.Error())
	}

	if !s.loader.Del(module.id) {
		return errors.New("failed to delete " + name + " Module State from loader")
	}

	return nil
}

// updateModule is internal function for update a module
func (s *sController) updateModule(module *Module) error {
	if err := s.stopModule(module); err != nil {
		return err
	}

	if err := s.startModule(module); err != nil {
		return err
	}

	return nil
}

func (s *sController) notifyModules(agentID string) {
	for aID, updateList := range s.updates {
		if agentID == aID || agentID == "" || aID == "" {
			for _, update := range updateList {
				update <- struct{}{}
			}
		}
	}
}

func (s *sController) checkStartModules(config []*loader.ModuleConfig) {
	var wantStartModules []*loader.ModuleConfig

	for _, mc := range config {
		var mct *loader.ModuleConfig
		id := mc.AgentID + ":" + mc.Name
		for _, module := range s.modules {
			if module.id == id {
				mct = mc
				break
			}
		}
		if mct == nil {
			wantStartModules = append(wantStartModules, mc)
		}
	}

	if len(wantStartModules) == 0 {
		return
	}

	files, err := s.files.load(wantStartModules)
	if err != nil {
		return
	}
	if len(wantStartModules) != len(files) {
		return
	}

	mupdate := make(map[string]struct{})
	for idx, mc := range wantStartModules {
		id := mc.AgentID + ":" + mc.Name
		module := s.newModule(mc, files[idx])
		if s.loader.Get(id) != nil {
			continue
		}

		if err := s.startModule(module); err != nil {
			continue
		}

		s.modules = append(s.modules, module)
		mupdate[mc.AgentID] = struct{}{}
	}

	if len(mupdate) > 0 {
		for agentID := range mupdate {
			s.notifyModules(agentID)
		}
	}
}

func (s *sController) checkStopModules(config []*loader.ModuleConfig) {
	var wantStopModules []*loader.ModuleConfig

	for _, module := range s.modules {
		var mct *loader.ModuleConfig
		for _, mc := range config {
			id := mc.AgentID + ":" + mc.Name
			if module.id == id {
				mct = mc
				break
			}
		}
		if mct == nil {
			wantStopModules = append(wantStopModules, module.GetConfig())
		}
	}

	if len(wantStopModules) == 0 {
		return
	}

	mupdate := make(map[string]struct{})
	for _, mc := range wantStopModules {
		id := mc.AgentID + ":" + mc.Name
		for mdx, module := range s.modules {
			if module.id == id {
				if s.loader.Get(id) == nil {
					continue
				}

				if err := s.stopModule(module); err != nil {
					break
				}

				mupdate[mc.AgentID] = struct{}{}
				// It's possible because there used break in the bottom
				s.modules = append(s.modules[:mdx], s.modules[mdx+1:]...)
				break
			}
		}
	}

	if len(mupdate) > 0 {
		for agentID := range mupdate {
			s.notifyModules(agentID)
		}
	}
}

func (s *sController) checkUpdateModules(config []*loader.ModuleConfig) {
	var wantUpdateModules []*loader.ModuleConfig
	isEqual := func(mc1, mc2 *loader.ModuleConfig) bool {
		// TODO: may be it needs fixed to check version only
		if mc1.LastUpdate != mc2.LastUpdate || mc1.Version != mc2.Version {
			return false
		}
		return true
	}

	for _, mc := range config {
		id := mc.AgentID + ":" + mc.Name
		for _, module := range s.modules {
			if module.id == id && !isEqual(module.config, mc) {
				wantUpdateModules = append(wantUpdateModules, mc)
				break
			}
		}
	}

	if len(wantUpdateModules) == 0 {
		return
	}

	files, err := s.files.load(wantUpdateModules)
	if err != nil {
		return
	}
	if len(wantUpdateModules) != len(files) {
		return
	}

	mupdate := make(map[string]struct{})
	for idx, mc := range wantUpdateModules {
		id := mc.AgentID + ":" + mc.Name
		for mdx, module := range s.modules {
			if module.id == id {
				s.modules[mdx] = s.newModule(mc, files[idx])
				if s.loader.Get(id) == nil {
					break
				}

				if err := s.updateModule(s.modules[mdx]); err != nil {
					break
				}

				mupdate[mc.AgentID] = struct{}{}
				break
			}
		}
	}

	if len(mupdate) > 0 {
		for agentID := range mupdate {
			s.notifyModules(agentID)
		}
	}
}

func (s *sController) updaterModules() {
	defer s.wg.Done()
	defer func() { s.closed = true }()

	for {
		s.mutex.Lock()
		config, err := s.config.load()
		if err == nil {
			s.checkUpdateModules(config)
			s.checkStopModules(config)
			s.checkStartModules(config)
		}
		s.mutex.Unlock()

		select {
		case <-time.NewTimer(time.Second * time.Duration(10)).C:
			continue
		case <-s.quit:
			break
		}

		break
	}
}
