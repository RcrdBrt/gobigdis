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
package internal

import (
	"fmt"
	"strconv"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/storage"
)

type HandlerFn func(r *Request) error

func NewV1Handler() map[string]HandlerFn {
	m := make(map[string]HandlerFn)

	m["ping"] = func(r *Request) error {
		reply := &StatusReply{
			Code: "PONG",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["select"] = func(r *Request) error {
		dbNum, err := strconv.Atoi(string(r.Args[0]))
		if err != nil {
			return err
		}

		var reply ReplyWriter
		if dbNum > config.Config.DBConfig.DBMaxNum-1 || dbNum < 0 {
			reply = &BulkReply{
				value: []byte(""),
			}
		} else {
			reply = &StatusReply{
				Code: "OK",
			}

			if err := storage.NewDB(dbNum); err != nil {
				return err
			}
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["command"] = func(r *Request) error {
		reply := &StatusReply{
			Code: "Welcome to GoBigDis",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["get"] = func(r *Request) error {
		value, err := storage.Get(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &BulkReply{
			value: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["set"] = func(r *Request) error {
		if err := storage.Set(r.GetDBNum(), r.Args); err != nil {
			return err
		}

		reply := &StatusReply{
			Code: "OK",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["flushdb"] = func(r *Request) error {
		if err := storage.FlushDB(r.GetDBNum()); err != nil {
			return err
		}

		reply := &StatusReply{
			Code: "OK",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["del"] = func(r *Request) error {
		deleted, err := storage.Del(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := IntegerReply{
			number: deleted,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["config"] = func(r *Request) error {
		reply := BulkReply{
			value: []byte(""),
		}

		fmt.Println(string(r.Args[1]))

		// switch string(r.Args[0]) {
		// case "get":
		// 	switch string(r.Args[1]) {
		// 	case "save":
		// 		reply.value = ""
		// 	}
		// }

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["hget"] = func(r *Request) error {

		return nil
	}

	m["hset"] = func(r *Request) error {

		return nil
	}

	return m
}
