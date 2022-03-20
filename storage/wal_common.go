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

//    Copyright 2018 Google Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

// see: https://github.com/danchia/ddb/blob/master/wal

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/utils"
)

func cleanObsoleteLog(applied int) {
	logFiles, err := listLog()
	if err != nil {
		utils.Debugf("cleanObsoleteLog: %s", err)
		return
	}

	cleaned := 0
	for i, logFile := range logFiles {
		seqNo, err := strconv.Atoi(strings.Split(strings.Split(logFile, "-")[1], ".")[0])
		if err != nil {
			utils.Debugf("cleanObsoleteLog: %s", err)
			continue
		}

		if seqNo < applied && i > 0 {
			// can delete the file
			if err := os.Remove(filepath.Join(config.Config.DBConfig.WalPath, logFile)); err != nil {
				utils.Debugf("cleanObsoleteLog: %s", err)
				return
			}

			cleaned++
		}
	}
}

func listLog() ([]string, error) {
	entries, err := os.ReadDir(config.Config.DBConfig.WalPath)
	if err != nil {
		return nil, err
	}

	var logFiles []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "wal-") || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}

		logFiles = append(logFiles, entry.Name())
	}

	sort.Strings(logFiles)

	return logFiles, nil
}

func RecoverLog(lastApplied uint64) (int, error) {
	sc, err := NewScanner()
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("No WAL files found")
			return 0, nil
		}
		return 0, err
	}

	var n, applied int
	seqNo := lastApplied

	for sc.Scan() {
		record := sc.Record()
		n++

		utils.Debugf("Reading wal record %s", record)

		if record.seq <= seqNo {
			continue
		}

		applied++

		seqNo = record.seq

	}

}
