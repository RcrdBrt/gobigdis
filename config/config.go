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
package config

/*
	CacheDepth is a const var just for documentation purposes.
	It avoids adding "3" as a condition for for-cycles, so the cycles' purpose
	is somewhat clearer.
	It is not supposed to be ever changed as it's a hard-coded feature.
*/
const CacheDepth = 3

type config struct {
	DBMaxNum int
}

var Config config

func Init() {
	Config = config{
		DBMaxNum: 16,
	}
}
