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
)

const MaxKeySize = 1024 * 8 // 8 KiB

const STORAGE_VERSION = 1

//go:embed default.json
var defaultConfig []byte

type config struct {
	DBConfig *struct {
		DBDirPath       string `json:"path"`
		DBMaxNum        int    `json:"max_num"`
		InternalDirPath string
	} `json:"db,omitempty"`
	ServerConfig *struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	} `json:"server,omitempty"`
}

var Config config

// Init bootstraps the config from the config file and warms up the DB dir
func Init(configFile string) {
	if configFile == "" {
		Config = parseAndValidate(defaultConfig)
	} else {
		content, err := os.ReadFile(configFile)
		if err != nil {
			log.Fatal(err)
		}

		Config = parseAndValidate(content)
	}

	if err := os.MkdirAll(Config.DBConfig.DBDirPath, 0700); err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(Config.DBConfig.InternalDirPath, 0700); err != nil {
		log.Fatal(err)
	}
}

// parse fills the config fields with the right stuff
func parseAndValidate(configFileContent []byte) config {
	var c config
	if err := json.Unmarshal(configFileContent, &c); err != nil {
		log.Fatal(err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}

	if c.DBConfig == nil {
		// section "db" does not exist in the config file
		c.DBConfig.DBDirPath = filepath.Join(home, "gobigdis")
		c.DBConfig.DBMaxNum = 16
	} else {
		// section "db" exists but could have some invalid fields
		if c.DBConfig.DBDirPath == "" {
			c.DBConfig.DBDirPath = filepath.Join(home, ".gobigdis")
		}

		if c.DBConfig.DBMaxNum < 1 {
			c.DBConfig.DBMaxNum = 16 // sane default
		}
	}

	c.DBConfig.InternalDirPath = filepath.Join(c.DBConfig.DBDirPath, "_internal")

	if c.ServerConfig == nil {
		// section "server" does not exist in the config file
		c.ServerConfig.Host = "127.0.0.1"
		c.ServerConfig.Port = 6379
	} else {
		// section "server" exists but has some invalid fields
		if c.ServerConfig.Host == "" {
			c.ServerConfig.Host = "127.0.0.1"
		}

		if c.ServerConfig.Port < 1 {
			c.ServerConfig.Port = 6389
		}
	}

	return c
}
