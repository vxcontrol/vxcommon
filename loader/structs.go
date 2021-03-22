package loader

import "strings"

// ModuleConfig is struct for contains module configuration
type ModuleConfig struct {
	AgentID     string              `json:"agent_id,omitempty"`
	OS          map[string][]string `json:"os"`
	Name        string              `json:"name"`
	Version     string              `json:"version"`
	Events      []string            `json:"events"`
	LastUpdate  string              `json:"last_update"`
	IConfigItem `json:"-"`
}

// IConfigItem is common interface for manage module configuration
type IConfigItem interface {
	GetConfigSchema() string
	GetDefaultConfig() string
	GetCurrentConfig() string
	SetCurrentConfig(string) bool
	GetEventDataSchema() string
	GetEventConfigSchema() string
	GetDefaultEventConfig() string
	GetCurrentEventConfig() string
}

// ModuleConfigItem is a static configuration which loaded from protobuf
type ModuleConfigItem struct {
	ConfigSchema       string
	DefaultConfig      string
	CurrentConfig      string
	EventDataSchema    string
	EventConfigSchema  string
	DefaultEventConfig string
	CurrentEventConfig string
}

// ModuleItem is struct that contains files and args for each module
type ModuleItem struct {
	args  map[string][]string
	files map[string][]byte
}

// ModuleFiles is struct that contains cmodule and smodule files
type ModuleFiles struct {
	smodule *ModuleItem
	cmodule *ModuleItem
}

// NewFiles is function that construct module files
func NewFiles() *ModuleFiles {
	return &ModuleFiles{
		smodule: NewItem(),
		cmodule: NewItem(),
	}
}

// NewItem is function that construct module item
func NewItem() *ModuleItem {
	var mi ModuleItem
	mi.args = make(map[string][]string)
	mi.files = make(map[string][]byte)

	return &mi
}

// GetConfigSchema is function which return schema module config
func (mci *ModuleConfigItem) GetConfigSchema() string {
	return mci.ConfigSchema
}

// GetDefaultConfig is function which return default module config
func (mci *ModuleConfigItem) GetDefaultConfig() string {
	return mci.DefaultConfig
}

// GetCurrentConfig is function which return current module config
func (mci *ModuleConfigItem) GetCurrentConfig() string {
	return mci.CurrentConfig
}

// SetCurrentConfig is function which store new module config to item
func (mci *ModuleConfigItem) SetCurrentConfig(config string) bool {
	mci.CurrentConfig = config
	return true
}

// GetEventDataSchema is function which return schema data config
func (mci *ModuleConfigItem) GetEventDataSchema() string {
	return mci.EventDataSchema
}

// GetEventConfigSchema is function which return schema event config
func (mci *ModuleConfigItem) GetEventConfigSchema() string {
	return mci.EventConfigSchema
}

// GetDefaultEventConfig is function which return default event config
func (mci *ModuleConfigItem) GetDefaultEventConfig() string {
	return mci.DefaultEventConfig
}

// GetCurrentEventConfig is function which return current event config
func (mci *ModuleConfigItem) GetCurrentEventConfig() string {
	return mci.CurrentEventConfig
}

// GetFiles is function which return files structure
func (mi *ModuleItem) GetFiles() map[string][]byte {
	return mi.files
}

// GetFilesByFilter is function which return files structure
func (mi *ModuleItem) GetFilesByFilter(os, arch string) map[string][]byte {
	var strictPrefix string
	clibsPrefix := "clibs/"
	mfiles := make(map[string][]byte)

	if os == "" {
		strictPrefix = clibsPrefix
	} else if arch == "" {
		strictPrefix = clibsPrefix + os + "/"
	} else {
		strictPrefix = clibsPrefix + os + "/" + arch + "/"
	}

	for path, data := range mi.files {
		if strings.HasPrefix(path, clibsPrefix) && !strings.HasPrefix(path, strictPrefix) {
			continue
		}
		mfiles[path] = data
	}

	return mfiles
}

// GetArgs is function which return arguments structure
func (mi *ModuleItem) GetArgs() map[string][]string {
	return mi.args
}

// SetFiles is function which store files structure to item
func (mi *ModuleItem) SetFiles(files map[string][]byte) {
	mi.files = files
}

// SetArgs is function which store arguments structure to item
func (mi *ModuleItem) SetArgs(args map[string][]string) {
	mi.args = args
}

// GetSModule is function which return server modules structure
func (mf *ModuleFiles) GetSModule() *ModuleItem {
	return mf.smodule
}

// GetCModule is function which return client modules structure
func (mf *ModuleFiles) GetCModule() *ModuleItem {
	return mf.cmodule
}

// SetSModule is function which store server modules structure
func (mf *ModuleFiles) SetSModule(mi *ModuleItem) {
	mf.smodule = mi
}

// SetCModule is function which store client modules structure
func (mf *ModuleFiles) SetCModule(mi *ModuleItem) {
	mf.cmodule = mi
}
