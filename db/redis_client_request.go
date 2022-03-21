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
package db

import (
	"net"
	"strconv"
)

type redisClientRequest struct {
	DB   [][]byte
	Name string
	Args [][]byte
	Conn net.Conn
}

func (r *redisClientRequest) GetDBNum() int {
	if len(r.DB) < 1 {
		return 0
	}

	dbNum, err := strconv.Atoi(string(r.DB[0]))
	if err != nil {
		return 0
	}

	return dbNum
}
