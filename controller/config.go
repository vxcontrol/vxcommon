package controller

type getCallback func() string
type setCallback func(string) bool

// sConfig is universal container for modules configuration loader
type sConfig struct {
	clt tConfigLoaderType
	IConfigLoader
}

// sConfigItem is struct for contains schema, default and current config data
type sConfigItem struct {
	getConfigSchema       getCallback
	getDefaultConfig      getCallback
	getCurrentConfig      getCallback
	setCurrentConfig      setCallback
	getEventDataSchema    getCallback
	getEventConfigSchema  getCallback
	getDefaultEventConfig getCallback
	getCurrentEventConfig getCallback
}

// GetConfigSchema is function which return JSON schema data of config as string
func (ci *sConfigItem) GetConfigSchema() string {
	if ci.getConfigSchema != nil {
		return ci.getConfigSchema()
	}

	return ""
}

// GetDefaultConfig is function which return default config data as string
func (ci *sConfigItem) GetDefaultConfig() string {
	if ci.getDefaultConfig != nil {
		return ci.getDefaultConfig()
	}

	return ""
}

// GetCurrentConfig is function which return current config data as string
func (ci *sConfigItem) GetCurrentConfig() string {
	if ci.getCurrentConfig != nil {
		return ci.getCurrentConfig()
	}

	return ""
}

// SetCurrentConfig is function which store this string config data to storage
func (ci *sConfigItem) SetCurrentConfig(config string) bool {
	if ci.setCurrentConfig != nil {
		return ci.setCurrentConfig(config)
	}

	return false
}

// GetEventDataSchema is function which return JSON schema of event data as string
func (ci *sConfigItem) GetEventDataSchema() string {
	if ci.getEventDataSchema != nil {
		return ci.getEventDataSchema()
	}

	return ""
}

// GetEventConfigSchema is function which return JSON schema data of event config as string
func (ci *sConfigItem) GetEventConfigSchema() string {
	if ci.getEventConfigSchema != nil {
		return ci.getEventConfigSchema()
	}

	return ""
}

// GetDefaultEventConfig is function which return default event config data as string
func (ci *sConfigItem) GetDefaultEventConfig() string {
	if ci.getDefaultEventConfig != nil {
		return ci.getDefaultEventConfig()
	}

	return ""
}

// GetCurrentEventConfig is function which return current event config data as string
func (ci *sConfigItem) GetCurrentEventConfig() string {
	if ci.getCurrentEventConfig != nil {
		return ci.getCurrentEventConfig()
	}

	return ""
}

// NewConfigFromDB is function which constructed Configuration loader object
func NewConfigFromDB() (IConfigLoader, error) {
	return &sConfig{
		clt:           eDBConfigLoader,
		IConfigLoader: &configLoaderDB{},
	}, nil
}

// NewConfigFromS3 is function which constructed Configuration loader object
func NewConfigFromS3() (IConfigLoader, error) {
	return &sConfig{
		clt:           eS3ConfigLoader,
		IConfigLoader: &configLoaderS3{},
	}, nil
}

// NewConfigFromFS is function which constructed Configuration loader object
func NewConfigFromFS(path string) (IConfigLoader, error) {
	return &sConfig{
		clt:           eFSConfigLoader,
		IConfigLoader: &configLoaderFS{path: path},
	}, nil
}
