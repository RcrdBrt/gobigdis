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

package storage

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/RcrdBrt/gobigdis/alg"
	"github.com/RcrdBrt/gobigdis/config"
)

var cache *alg.Cache

func Init() {
	versionFile := filepath.Join(config.Config.DBConfig.InternalDirPath, "VERSION")

	if _, err := os.Stat(versionFile); err != nil {
		if os.IsNotExist(err) {
			if err := os.WriteFile(versionFile, []byte(config.Config.DBConfig.Version), 0600); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	} else {
		// VERSION file exists
		versionFileContent, err := os.ReadFile(versionFile)
		if err != nil {
			log.Fatal(err)
		}

		versionNumberFound, err := strconv.Atoi(string(versionFileContent))
		if err != nil {
			log.Fatal(err)
		}

		versionNumberCurrent, err := strconv.Atoi(config.Config.DBConfig.Version)
		if err != nil {
			log.Fatal(err)
		}

		if versionNumberCurrent > versionNumberFound {
			if err := migrate(versionNumberFound, versionNumberCurrent); err != nil {
				log.Fatal(err)
			}
		}
	}

	cache = &alg.Cache{
		MaxDBNum: config.Config.DBConfig.DBMaxNum,
		Root:     config.Config.DBConfig.DBDirName,
	}
	cache.BuildCacheData()

	go cache.Vacuum(config.Config.DBConfig.DBMaxNum, 10*time.Minute)
}

func NewDB(dbNum int) error {
	dbPath := filepath.Join(config.Config.DBConfig.DBDirPath, strconv.FormatInt(int64(dbNum), 10))

	cache.FSRWL.Lock()
	defer cache.FSRWL.Unlock()

	if err := os.MkdirAll(dbPath, 0700); err != nil {
		return err
	}

	return nil
}

func FlushDB(dbNum int) error {
	cache.FSRWL.Lock()
	defer cache.FSRWL.Unlock()

	if err := os.RemoveAll(filepath.Join(config.Config.DBConfig.DBDirPath, strconv.FormatInt(int64(dbNum), 10))); err != nil {
		return err
	}

	return nil
}
