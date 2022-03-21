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
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/utils"
)

type redisTCPServer struct {
	host         string
	port         int
	monitorChans []chan string
	methods      map[string]handlerFn
	listener     *net.TCPListener
}

func startRedisTCPServer() {
	srv := &redisTCPServer{
		host:         config.Config.ServerConfig.Host,
		port:         config.Config.ServerConfig.Port,
		monitorChans: []chan string{},
	}

	srv.methods = newHandlerV1()

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   net.ParseIP(srv.host),
		Port: srv.port,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	srv.listener = listener

	srv.monitorChans = []chan string{} // empty slice is 0-sized

	for {
		conn, err := srv.listener.AcceptTCP()
		if err != nil {
			log.Fatal(err)
		}

		go srv.serveClient(conn)
	}
}

func (srv *redisTCPServer) serveClient(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(conn, "-%s\r\n", err)
		}
		if err := conn.Close(); err != nil {
			log.Println(err)
		}
	}()

	reader := bufio.NewReader(conn)
	dbNum := [][]byte{[]byte("0")}
	for {
		request, err := parseRequest(reader)
		if err != nil {
			panic(err)
		}
		request.Conn = conn

		if request.Name == "select" {
			dbNum = request.Args
		}
		request.DB = dbNum

		if request.Name == "quit" {
			fmt.Fprint(conn, "+OK\r\n")
			return
		}

		utils.Debugf("db %d: '%s' '%s'\n", request.GetDBNum(), request.Name, request.Args)

		if err := srv.methods[request.Name](request); err != nil {
			panic(err)
		}
	}
}
