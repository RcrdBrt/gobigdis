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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/RcrdBrt/gobigdis/config"
)

var fsLock sync.RWMutex

func Init() {
	versionFilePath := filepath.Join(config.Config.DBConfig.InternalDirPath, "VERSION")

	versionFile, err := os.OpenFile(versionFilePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer versionFile.Close()

	prevVersionFileContent, err := os.ReadFile(versionFilePath)
	if err != nil {
		log.Fatal(err)
	}

	if len(prevVersionFileContent) > 0 {
		// file already existed
		versionNumberFound, err := strconv.Atoi(string(prevVersionFileContent))
		if err != nil {
			log.Fatal(err)
		}

		// storage migration
		if config.STORAGE_VERSION > versionNumberFound {
			if err := migrate(versionNumberFound, config.STORAGE_VERSION); err != nil {
				log.Fatal(err)
			}
		}
	}

	if _, err := versionFile.Write([]byte(fmt.Sprint(config.STORAGE_VERSION))); err != nil {
		log.Fatal(err)
	}

	if err := versionFile.Sync(); err != nil {
		log.Fatal(err)
	}
}

func NewDB(dbNum int) error {
	dbPath := filepath.Join(config.Config.DBConfig.DBDirPath, fmt.Sprint(dbNum))

	if err := os.MkdirAll(dbPath, 0700); err != nil {
		return err
	}

	return nil
}

func FlushDB(dbNum int) error {
	if err := os.RemoveAll(filepath.Join(config.Config.DBConfig.DBDirPath, fmt.Sprint(dbNum))); err != nil {
		return err
	}

	return nil
}
