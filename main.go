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
package main

import (
	"flag"
	"log"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/network"
	"github.com/RcrdBrt/gobigdis/storage"
)

var (
	host       = flag.String("h", "localhost", "IP address to listen on")
	port       = flag.Int("p", 6389, "Port of the socket")
	dbRoot     = flag.String("d", "", "Database root folder")
	configFile = flag.String("c", "", "Path to the config file")
)

func main() {
	flag.Parse()

	config.Init(*configFile, *dbRoot, *host, *port)

	storage.Init()

	log.Fatal(network.StartServer())
}
