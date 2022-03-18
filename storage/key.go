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
	"os"
	"path/filepath"
)

func Get(dbNum int, args [][]byte) ([]byte, error) {
	fsLock.RLock()
	defer fsLock.RUnlock()

	path := pathFromKey(dbNum, args[0])

	value, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	return value, nil
}

func Set(dbNum int, args [][]byte) error {
	fsLock.Lock()
	defer fsLock.Unlock()

	path := pathFromKey(dbNum, args[0])

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}

	if err := os.WriteFile(path, args[1], 0600); err != nil {
		return err
	}

	return nil
}

func Del(dbNum int, args [][]byte) (int, error) {
	fsLock.Lock()
	defer fsLock.Unlock()

	var deleted int
	// best-effort deletion, doesn't revert in case of mid-iteration errors
	for _, v := range args {
		path := pathFromKey(dbNum, v)

		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return deleted, err
		}

		deleted++
	}

	return deleted, nil
}
