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
	"time"

	"github.com/RcrdBrt/GoBigdis/alg"
)

type ExpiringKey struct {
	alg.Key
	Expire time.Time
}

func Get(dbNum int, args [][]byte) ([]byte, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("wrong command syntax")
	}

	key := cache.NewKey(dbNum, args[0])

	cache.FSRWL.RLock()
	defer cache.FSRWL.RUnlock()

	if !cache.Match(key) {
		return nil, nil
	}

	value, err := os.ReadFile(key.FilePath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	return value, nil
}

func Set(dbNum int, args [][]byte) error {
	if len(args) < 2 {
		return fmt.Errorf("wrong command syntax")
	}

	cache.FSRWL.Lock()
	defer cache.FSRWL.Unlock()

	key := cache.NewKey(dbNum, args[0])

	if !cache.Match(key) {
		if err := os.MkdirAll(key.ParentPath(), 0700); err != nil {
			return err
		}

		cache.Add(key)
	}

	if err := os.WriteFile(key.FilePath(), args[1], 0600); err != nil {
		return err
	}

	return nil
}

func Del(dbNum int, args [][]byte) (int, error) {
	key := cache.NewKey(dbNum, args[0])

	cache.FSRWL.Lock()
	defer cache.FSRWL.Unlock()

	counter := 0

	if err := os.Remove(key.FilePath()); err != nil && !os.IsNotExist(err) {
		return counter, err
	} else {
		if !os.IsNotExist(err) {
			counter++
		}
	}

	go func() {
		cache.FSRWL.RLock()
		fileList, err := os.ReadDir(key.ParentPath())
		cache.FSRWL.RUnlock()
		if err != nil && !os.IsNotExist(err) {
			log.Println(err)
			return
		}

		if len(fileList) == 0 {
			// deepest folder is empty, update the cache
			cache.Set(key, false)
		}
	}()

	return counter, nil
}
