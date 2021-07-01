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
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strconv"
)

type Key struct {
	DB        int
	dbDirPath string   // root of the DB, same as c.Root
	HashedKey [32]byte // sha256 of the key
}

// NewKey is a convenience function for Key struct generation
func (c *Cache) NewKey(dbNum int, keyName []byte) Key {
	hashedKey := sha256.Sum256(keyName)

	result := Key{
		DB:        dbNum,
		dbDirPath: c.Root,
		HashedKey: hashedKey,
	}

	return result
}

// Level returns the hex-encoded string of the nth byte
func (k *Key) Level(nth int) string {
	return hex.EncodeToString([]byte{k.HashedKey[nth]})
}

// Encode encodes HashedKey field with encoding/hex
func (k *Key) Encode() string {
	return hex.EncodeToString(k.HashedKey[:])
}

// Level3 is a convenience function that returns the
// first 3 hex-encoded bytes of HashedKey
func (k *Key) Level3() []string {
	return []string{
		k.Level(0),
		k.Level(1),
		k.Level(2),
	}
}

func (k *Key) ParentPath() string {
	return filepath.Join(
		k.dbDirPath,
		strconv.FormatInt(int64(k.DB), 10),
		k.Level(0),
		k.Level(1),
		k.Level(2),
	)
}

func (k *Key) FilePath() string {
	return filepath.Join(
		k.dbDirPath,
		strconv.FormatInt(int64(k.DB), 10),
		k.Level(0),
		k.Level(1),
		k.Level(2),
		k.Encode(),
	)
}
