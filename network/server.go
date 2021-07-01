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
package network

import (
	"bufio"
	"fmt"
	"gobigdis/internal"
	"log"
	"net"
)

type server struct {
	host         string
	port         int
	monitorChans []chan string
	methods      map[string]internal.HandlerFn
	listener     *net.TCPListener
}

func StartServer(host string, port int) error {
	if host == "" {
		host = "localhost"
	}

	if port == 0 {
		port = 6389
	}

	srv := &server{
		host:         host,
		port:         port,
		monitorChans: []chan string{},
	}

	srv.methods = internal.NewV1Handler()

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   net.ParseIP(srv.host),
		Port: srv.port,
	})
	if err != nil {
		return err
	}
	defer listener.Close()

	srv.listener = listener

	srv.monitorChans = []chan string{}

	for {
		conn, err := srv.listener.AcceptTCP()
		if err != nil {
			return err
		}

		go srv.serveClient(conn)
	}
}

func (srv *server) serveClient(conn net.Conn) {
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

		if err := srv.methods[request.Name](request); err != nil {
			panic(err)
		}
	}
}
