package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/vxcontrol/vxcommon/db"
	"github.com/vxcontrol/vxcommon/loader"
	"github.com/vxcontrol/vxcommon/storage"
)

// tConfigLoaderType is type for loading config
type tConfigLoaderType int32

// Enum config loading types
const (
	eDBConfigLoader tConfigLoaderType = 0
	eS3ConfigLoader tConfigLoaderType = 1
	eFSConfigLoader tConfigLoaderType = 2
)

// List of SQL queries string
const (
	sLoadModulesSQL    string = "SELECT `id`, IF(`agent_id` = 0, '', (SELECT `hash` FROM `agents` WHERE `id` = `agent_id`)) AS `agent_id`, `info`, `last_update` FROM `modules` WHERE `status` = 'joined'"
	sGetModuleFieldSQL string = "SELECT `%s` FROM `modules` WHERE `id` = ? LIMIT 1"
	sSetModuleFieldSQL string = "UPDATE `modules` SET `%s` = ? WHERE `id` = ?"
)

// tFilesLoaderType is type for loading module
type tFilesLoaderType int32

// Enum files loading types
const (
	eS3FilesLoader tFilesLoaderType = 0
	eFSFilesLoader tFilesLoaderType = 1
)

// IConfigLoader is internal interface for loading config from external storage
type IConfigLoader interface {
	load() ([]*loader.ModuleConfig, error)
}

// IFilesLoader is internal interface for loading files from external storage
type IFilesLoader interface {
	load(mcl []*loader.ModuleConfig) ([]*loader.ModuleFiles, error)
}

// configLoaderDB is container for config which loaded from DB
type configLoaderDB struct {
}

func (cl *configLoaderDB) getCb(id, col string) getCallback {
	sql := fmt.Sprintf(sGetModuleFieldSQL, col)
	return func() string {
		db, err := db.New()
		if err != nil {
			return ""
		}

		rows, err := db.Query(sql, id)
		if err != nil {
			return ""
		}
		if len(rows) != 1 {
			return ""
		}
		if data, ok := rows[0][col]; ok {
			return data
		}

		return ""
	}
}

func (cl *configLoaderDB) setCb(id, col string) setCallback {
	sql := fmt.Sprintf(sSetModuleFieldSQL, col)
	return func(val string) bool {
		db, err := db.New()
		if err != nil {
			return false
		}

		_, err = db.Exec(sql, val, id)
		if err != nil {
			return false
		}

		return true
	}
}

func joinPath(args ...string) string {
	tpath := filepath.Join(args...)
	return strings.Replace(tpath, "\\", "/", -1)
}

// load is function what retrieve modules config list from DB
func (cl *configLoaderDB) load() ([]*loader.ModuleConfig, error) {
	db, err := db.New()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(sLoadModulesSQL)
	if err != nil {
		return nil, err
	}

	var ml []*loader.ModuleConfig
	for _, m := range rows {
		var mc loader.ModuleConfig
		if info, ok := m["info"]; ok {
			if err = json.Unmarshal([]byte(info), &mc); err != nil {
				return nil, errors.New("error parsing module config: " + err.Error())
			}
			if agentID, ok := m["agent_id"]; ok {
				mc.AgentID = agentID
			}
			if lastUpdate, ok := m["last_update"]; ok {
				mc.LastUpdate = lastUpdate
			}
		} else {
			return nil, errors.New("failed loading module config")
		}
		ci := &sConfigItem{}
		if id, ok := m["id"]; ok {
			ci.getConfigSchema = cl.getCb(id, "config_schema")
			ci.getDefaultConfig = cl.getCb(id, "default_config")
			ci.getCurrentConfig = cl.getCb(id, "current_config")
			ci.setCurrentConfig = cl.setCb(id, "current_config")
			ci.getEventDataSchema = cl.getCb(id, "event_data_schema")
			ci.getEventConfigSchema = cl.getCb(id, "event_config_schema")
			ci.getDefaultEventConfig = cl.getCb(id, "default_event_config")
			ci.getCurrentEventConfig = cl.getCb(id, "current_event_config")
		}
		mc.IConfigItem = ci
		ml = append(ml, &mc)
	}

	return ml, nil
}

func getStorageCb(s storage.IStorage, mpath, file string) getCallback {
	return func() string {
		data, err := s.ReadFile(joinPath(mpath, file))
		if err == nil {
			return string(data)
		}

		return ""
	}
}

func setStorageCb(s storage.IStorage, mpath, file string) setCallback {
	return func(val string) bool {
		if s.WriteFile(joinPath(mpath, file), []byte(val)) != nil {
			return false
		}

		return true
	}
}

func loadConfig(s storage.IStorage, path string) ([]*loader.ModuleConfig, error) {
	var mcl []*loader.ModuleConfig
	cpath := strings.Replace(filepath.Join(path, "config.json"), "\\", "/", -1)
	if s.IsNotExist(path) {
		return nil, errors.New("directory with config not found")
	}
	if s.IsNotExist(cpath) {
		return nil, errors.New("config file not found")
	}
	cdata, err := s.ReadFile(cpath)
	if err != nil {
		return nil, errors.New("failed read config file: " + err.Error())
	}
	if err = json.Unmarshal(cdata, &mcl); err != nil {
		return nil, errors.New("error while reading modules config: " + err.Error())
	}
	for _, mc := range mcl {
		mpath := joinPath(path, mc.Name, mc.Version, "config")
		ci := &sConfigItem{
			getConfigSchema:       getStorageCb(s, mpath, "config_schema.json"),
			getDefaultConfig:      getStorageCb(s, mpath, "default_config.json"),
			getCurrentConfig:      getStorageCb(s, mpath, "current_config.json"),
			setCurrentConfig:      setStorageCb(s, mpath, "current_config.json"),
			getEventDataSchema:    getStorageCb(s, mpath, "event_data_schema.json"),
			getEventConfigSchema:  getStorageCb(s, mpath, "event_config_schema.json"),
			getDefaultEventConfig: getStorageCb(s, mpath, "default_event_config.json"),
			getCurrentEventConfig: getStorageCb(s, mpath, "current_event_config.json"),
		}
		mc.IConfigItem = ci
	}

	return mcl, nil
}

// configLoaderS3 is container for config which loaded from D3
type configLoaderS3 struct {
}

// load is function what retrieve modules config list from S3
func (cl *configLoaderS3) load() ([]*loader.ModuleConfig, error) {
	s, err := storage.NewS3()
	if err != nil {
		return nil, errors.New("failed initialize S3 driver: " + err.Error())
	}

	return loadConfig(s, "/")
}

// configLoaderFS is container for config which loaded from FS
type configLoaderFS struct {
	path string
}

// load is function what retrieve modules config list from FS
func (cl *configLoaderFS) load() ([]*loader.ModuleConfig, error) {
	fs, err := storage.NewFS()
	if err != nil {
		return nil, errors.New("failed initialize FS driver: " + err.Error())
	}

	return loadConfig(fs, cl.path)
}

func removeLeadSlash(files map[string][]byte) map[string][]byte {
	rfiles := make(map[string][]byte)
	for name, data := range files {
		rfiles[name[1:]] = data
	}
	return rfiles
}

func loadUtils(s storage.IStorage, path string) (map[string][]byte, error) {
	var err error
	upath := joinPath(path, "utils")
	if s.IsNotExist(upath) {
		return nil, errors.New("utils directory not found")
	}

	files, err := s.ReadDirRec(upath)
	if err != nil {
		return nil, errors.New("failed read utils files: " + err.Error())
	}
	files = removeLeadSlash(files)

	return files, nil
}

func loadFiles(s storage.IStorage, path string, mcl []*loader.ModuleConfig) ([]*loader.ModuleFiles, error) {
	var mfl []*loader.ModuleFiles
	if s.IsNotExist(path) {
		return nil, errors.New("directory with modules not found")
	}

	utils, err := loadUtils(s, path)
	if err != nil {
		return nil, err
	}

	for _, mc := range mcl {
		var mf loader.ModuleFiles
		loadModuleDir := func(dir string) (*loader.ModuleItem, error) {
			mpath := joinPath(path, mc.Name, mc.Version, dir)
			if s.IsNotExist(mpath) {
				return nil, errors.New("module directory not found")
			}
			var mi loader.ModuleItem
			files, err := s.ReadDirRec(mpath)
			if err != nil {
				return nil, errors.New("failed read module files: " + err.Error())
			}
			files = removeLeadSlash(files)
			for p, d := range utils {
				if _, ok := files[p]; !ok {
					files[p] = d
				}
			}
			args := make(map[string][]string)
			if data, ok := files["args.json"]; ok {
				if err = json.Unmarshal(data, &args); err != nil {
					return nil, errors.New("failed read module args: " + err.Error())
				}
			}
			mi.SetArgs(args)
			mi.SetFiles(files)

			return &mi, nil
		}

		var smi, cmi *loader.ModuleItem
		if cmi, err = loadModuleDir("cmodule"); err != nil {
			return nil, err
		}
		if smi, err = loadModuleDir("smodule"); err != nil {
			return nil, err
		}
		mf.SetCModule(cmi)
		mf.SetSModule(smi)
		mfl = append(mfl, &mf)
	}

	return mfl, nil
}

// filesLoaderS3 is container for files structure which loaded from S3
type filesLoaderS3 struct {
}

// load is function what retrieve modules files data from S3
func (fl *filesLoaderS3) load(mcl []*loader.ModuleConfig) ([]*loader.ModuleFiles, error) {
	s, err := storage.NewS3()
	if err != nil {
		return nil, errors.New("failed initialize S3 driver: " + err.Error())
	}

	return loadFiles(s, "/", mcl)
}

// filesLoaderFS is container for files structure which loaded from FS
type filesLoaderFS struct {
	path string
}

// load is function what retrieve modules files data from FS
func (fl *filesLoaderFS) load(mcl []*loader.ModuleConfig) ([]*loader.ModuleFiles, error) {
	fs, err := storage.NewFS()
	if err != nil {
		return nil, errors.New("failed initialize FS driver: " + err.Error())
	}

	return loadFiles(fs, fl.path, mcl)
}
