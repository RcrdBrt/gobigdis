/*
	GoBigdis is a persistent database that implements the Redis server protocol.
    Copyright (C) 2021  Riccardo Berto

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
package config

import (
	_ "embed"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
)

/*
	CacheDepth is a const var just for documentation purposes.
	It avoids adding "3" as a condition for for-cycles, so the cycles' purpose
	is somewhat clearer.
	It is not supposed to be ever changed as it's a hard-coded feature.
*/
const CacheDepth = 3

//go:embed default.json
var defaultConfig []byte

type dbConfig struct {
	DBDirPath       string `json:"db_dir"`
	DBMaxNum        int    `json:"db_max_num"`
	DBDirName       string
	InternalDirPath string
	Version         string
}

type serverConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type config struct {
	DBConfig     *dbConfig     `json:"db,omitempty"`
	ServerConfig *serverConfig `json:"server,omitempty"`
}

var Config config

// Init bootstraps the config from the config file and warms up the DB dir
func Init(configFile, dbRoot, host string, port int) {
	if configFile == "" {
		Config = parse(defaultConfig)
	} else {
		content, err := os.ReadFile(configFile)
		if err != nil {
			log.Fatal(err)
		}

		Config = parse(content)
	}

	if dbRoot != "" {
		Config.DBConfig.DBDirPath = dbRoot
		Config.DBConfig.DBDirName = strings.Split(Config.DBConfig.DBDirPath, string(filepath.Separator))[len(Config.DBConfig.DBDirPath)-1]
	}

	if host != "" {
		Config.ServerConfig.Host = host
	}

	if port != 0 {
		Config.ServerConfig.Port = port
	}

	if err := os.MkdirAll(Config.DBConfig.DBDirPath, 0700); err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(Config.DBConfig.InternalDirPath, 0700); err != nil {
		log.Fatal(err)
	}
}

// parse fills the config fields with the right stuff
func parse(configFileContent []byte) config {
	var c config
	if err := json.Unmarshal(configFileContent, &c); err != nil {
		log.Fatal(err)
	}

	if c.DBConfig == nil {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err)
		}
		c.DBConfig = &dbConfig{
			DBDirPath: filepath.Join(home, ".gobigdis"),
			DBMaxNum:  16,
			DBDirName: ".gobigdis",
			Version:   "1",
		}
	} else {
		c.DBConfig.DBDirName = strings.Split(c.DBConfig.DBDirPath, string(filepath.Separator))[len(c.DBConfig.DBDirPath)-1]
	}

	c.DBConfig.InternalDirPath = filepath.Join(c.DBConfig.DBDirPath, "_internal")

	if c.ServerConfig == nil {
		c.ServerConfig = &serverConfig{
			Host: "localhost",
			Port: 6389,
		}
	}

	return c
}
