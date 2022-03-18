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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"github.com/RcrdBrt/gobigdis/config"
)

func pathFromKey(dbNum int, key []byte) string {
	shaKey := sha256.Sum256(key)
	hashedKey := hex.EncodeToString(shaKey[:])

	return filepath.Join(config.Config.DBConfig.DBDirPath, fmt.Sprint(dbNum), hashedKey[:2], hashedKey[2:4], hashedKey)
}
