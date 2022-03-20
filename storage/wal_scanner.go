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

package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/RcrdBrt/gobigdis/config"
)

// Scanner reads log records from a write ahead log directory.
// Not thread-safe.
type Scanner struct {
	// list of log files to scan, in ascending seqNo.
	logs []string

	curIndex   int
	curScanner *fileScanner

	err error
}

// NewScanner returns a log scanner over all the log files found in the wal directory.
// Returns ErrNotExist if there are no log files.
func NewScanner() (*Scanner, error) {
	logs, err := listLog()
	if err != nil {
		return nil, err
	}

	return &Scanner{logs: logs}, nil
}

// Scan advances the fileScanner to the next log record, which will then be
// available through the Record method. It returns false when the scan stops,
// either by reaching the end of all logs or on error.
func (s *Scanner) Scan() bool {
	for {
		if !s.maybeAdvanceFileScanner() {
			return false
		}

		hasNext := s.curScanner.Scan()
		if hasNext {
			return true
		}
		if s.curScanner.Err() != nil {
			return false
		}
		// reached end of current file
		s.curScanner = nil
	}
}

// returns whether attempted advance was successful
func (s *Scanner) maybeAdvanceFileScanner() bool {
	if s.curScanner == nil {
		if s.curIndex >= len(s.logs) {
			return false
		}
		fi := s.logs[s.curIndex]
		s.curIndex++

		fileScanner, err := newFileScanner(filepath.Join(config.Config.DBConfig.WalPath, fi))
		if err != nil {
			s.err = err
			return false
		}
		s.curScanner = fileScanner
	}
	return true
}

// Record returns the current record.
// Only valid until the next Scan() call.
// Caller should not modify returned logRecord.
func (s *Scanner) Record() *logRecord {
	return s.curScanner.Record()
}

// Err returns last error, if any.
func (s *Scanner) Err() error {
	if s.err != nil {
		return s.err
	}
	if s.curScanner != nil {
		return s.curScanner.err
	}
	return nil
}

// fileScanner reads log records from a write ahead log.
// Not thread-safe.
type fileScanner struct {
	f   *os.File
	err error
	l   *logRecord
	h   hash.Hash32
}

func newFileScanner(name string) (*fileScanner, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	s := &fileScanner{
		f: f,
		l: &logRecord{},
		h: crc32.New(crcTable),
	}
	return s, nil
}

// Scan advances the fileScanner to the next log record, which will then be
// available through the Record method. It returns false when the scan stops,
// either by reaching the end of the log or on error.
func (s *fileScanner) Scan() bool {
	s.l.reset()

	var scratch [8]byte
	if _, s.err = io.ReadFull(s.f, scratch[:]); s.err != nil {
		if s.err == io.EOF {
			// Expected error.
			s.err = nil
		}
		return false
	}
	dataLen := binary.LittleEndian.Uint32(scratch[0:4])
	crc := binary.LittleEndian.Uint32(scratch[4:8])

	// TODO: reuse buffers
	data := make([]byte, dataLen, dataLen)

	if _, s.err = io.ReadFull(s.f, data); s.err != nil {
		return false
	}
	s.h.Reset()
	if _, s.err = s.h.Write(data); s.err != nil {
		return false
	}
	c := s.h.Sum32()
	if c != crc {
		s.err = fmt.Errorf("checksum mismatch. expected %d, got %d", crc, c)
		return false
	}

	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(s.l); err != nil {
		s.err = err
		return false
	}

	return true
}

// Returns the current record.
// Only valid until the next Scan() call.
// Caller should not modify returned logRecord.
func (s *fileScanner) Record() *logRecord {
	return s.l
}

// Returns last error, if any.
func (s *fileScanner) Err() error {
	return s.err
}
