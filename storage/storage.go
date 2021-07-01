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
	"strings"
	"time"

	"github.com/RcrdBrt/gobigdis/alg"
	"github.com/RcrdBrt/gobigdis/config"
)

var DBDirPath string

var dbDirName string

var cache *alg.Cache

func Init(dbroot string) {
	DBDirPath = dbroot
	if DBDirPath == "" {
		// no dbroot passed
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err)
		}

		DBDirPath = filepath.Join(home, ".gobigdis")
		dbDirName = ".gobigdis"
	} else {
		dbDirName = strings.Split(DBDirPath, string(filepath.Separator))[len(DBDirPath)-1]
	}

	if err := os.MkdirAll(DBDirPath, 0700); err != nil {
		log.Fatal(err)
	}

	cache = &alg.Cache{
		MaxDBNum: config.Config.DBMaxNum,
		Root:     DBDirPath,
	}
	cache.BuildCacheData()

	go cache.Vacuum(config.Config.DBMaxNum, 10*time.Minute)
}

func NewDB(dbNum int) error {
	dbPath := filepath.Join(DBDirPath, strconv.FormatInt(int64(dbNum), 10))

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

	if err := os.RemoveAll(filepath.Join(DBDirPath, strconv.FormatInt(int64(dbNum), 10))); err != nil {
		return err
	}

	return nil
}
