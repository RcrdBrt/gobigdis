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

// NewHandlerV1 implements the redis commands and sanitizes the input before calling the storage layer
func NewHandlerV1() map[string]HandlerFn {
	m := make(map[string]HandlerFn)

	m["ping"] = func(r *Request) error {
		reply := &StatusReply{
			code: "PONG",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["select"] = func(r *Request) error {
		var reply ReplyWriter
		if len(r.Args) != 1 {
			reply = &ErrorReply{
				msg: "ERR wrong number of arguments for 'select' command",
			}
		} else {
			dbNum, err := strconv.Atoi(string(r.Args[0]))
			if err != nil {
				return err
			}

			if dbNum > config.Config.DBConfig.DBMaxNum-1 || dbNum < 0 {
				reply = &ErrorReply{
					msg: "ERR DB index is out of range",
				}
			} else {
				reply = &StatusReply{
					code: "OK",
				}

				if err := storage.NewDB(dbNum); err != nil {
					return err
				}
			}
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["command"] = func(r *Request) error {
		// placeholder reply
		reply := &StatusReply{
			code: "Welcome to GoBigDis",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["get"] = func(r *Request) error {
		if len(r.Args) != 1 {
			return fmt.Errorf("ERR wrong number of arguments for 'get' command")
		}

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
		// TODO: expiration
		if len(r.Args) < 2 {
			return fmt.Errorf("ERR wrong number of arguments for 'set' command")
		}

		if err := storage.Set(r.GetDBNum(), r.Args); err != nil {
			return err
		}

		reply := &StatusReply{
			code: "OK",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["flushdb"] = func(r *Request) error {
		if len(r.Args) != 0 {
			return fmt.Errorf("ERR wrong number of arguments for 'flushdb' command")
		}

		if err := storage.FlushDB(r.GetDBNum()); err != nil {
			return err
		}

		reply := &StatusReply{
			code: "OK",
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
