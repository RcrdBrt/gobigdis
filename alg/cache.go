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
package alg

import (
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Cache struct {
	FSRWL        sync.RWMutex // RWMutex for filesystem access
	DataLock     sync.Mutex   // used only by writers to implement the copy-on-write pattern
	MaxDBNum     int          // the max number of the DBs to consider
	Root         string       // the parent folder of all the dbNum dirs
	Data         atomic.Value // [dbNum][first][second][third]bool, this is the field that represents the source of truth for the cache
	vacuumTicker *time.Ticker
}

// Match returns true if the first config.CacheDepth bytes
// of key match any bytes of the respective levels of the Cache
func (c *Cache) Match(key Key) bool {
	data := c.Data.Load().([][][][]bool)

	return data[key.DB][key.HashedKey[0]][key.HashedKey[1]][key.HashedKey[2]]
}

// expensive, must sync with other writers
func (c *Cache) Set(key Key, value bool) {
	c.DataLock.Lock() // sync with other writers
	defer c.DataLock.Unlock()

	data := c.Data.Load().([][][][]bool)

	data[key.DB][key.HashedKey[0]][key.HashedKey[1]][key.HashedKey[2]] = value

	c.Data.Store(data)
}

func (c *Cache) Add(key Key) {
	c.Set(key, true)
}

// BuildCacheData builds a new Cache index from the filesystem
func (c *Cache) BuildCacheData() {
	data := make([][][][]bool, c.MaxDBNum)
	for dbNum := 0; dbNum < c.MaxDBNum; dbNum++ {
		data[dbNum] = make([][][]bool, 256)
		for i := 0; i < 256; i++ {
			data[dbNum][i] = make([][]bool, 256)
			for j := 0; j < 256; j++ {
				data[dbNum][i][j] = make([]bool, 256)
				for k := 0; k < 256; k++ {
					data[dbNum][i][j][k] = false
				}
			}
		}
	}

	c.FSRWL.Lock() // Lock instead of RLock to prevent inconsistent inserts
	defer c.FSRWL.Unlock()

	// Lock here, start new copy of data (cow pattern)
	c.DataLock.Lock()
	defer c.DataLock.Unlock()

	for dbNum := 0; dbNum < c.MaxDBNum; dbNum++ {
		dbDirPath := filepath.Join(c.Root, strconv.FormatInt(int64(dbNum), 10))

		if err := filepath.WalkDir(dbDirPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

			hashedKey, err := hex.DecodeString(d.Name())
			if err != nil {
				return err
			}

			data[dbNum][hashedKey[0]][hashedKey[1]][hashedKey[2]] = true

			return nil
		}); err != nil {
			if !os.IsNotExist(err) {
				log.Fatal(err)
			}
		}
	}

	c.Data.Store(data)
}

// Vacuum keeps the structure in sync with the filesystem representation.
// It is needed for handling key removals. It must stop the world.
func (c *Cache) Vacuum(dbMaxNum int, d time.Duration) {
	c.vacuumTicker = time.NewTicker(d)
	for {
		<-c.vacuumTicker.C
		fmt.Println("VACUUM time!")
		c.BuildCacheData()
	}
}
