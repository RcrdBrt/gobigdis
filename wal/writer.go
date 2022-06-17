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

package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/utils"
)

// Writer writes log entries to the write ahead log.
// Thread-safe.
type Writer struct {
	sync.Mutex

	nextSeq  int64
	buf      *bytes.Buffer
	filename string
	size     int64

	f         *os.File
	bufWriter *bufio.Writer
	recordCh  chan rawRecord

	closeCh       chan struct{}
	closeResultCh chan error
}

func NewWriter(nextSeq int64) (*Writer, error) {
	writer := &Writer{
		buf:           bytes.NewBuffer(nil),
		nextSeq:       nextSeq,
		recordCh:      make(chan rawRecord, 1000),
		closeCh:       make(chan struct{}),
		closeResultCh: make(chan error),
		filename:      logName(nextSeq),
	}
	if err := writer.rollover(nextSeq); err != nil {
		return nil, err
	}
	go writer.writeLoop()
	return writer, nil
}

type rawRecord struct {
	seq      int64
	data     []byte
	checkSum uint32
	cb       func(error)
}

// Append appends a log record to the WAL. The log record is modified with the log sequence number.
// cb is invoked serially, in log sequence number order.
func (w *Writer) Append(l *LogRecord, cb func(error)) {
	utils.Debugf("wal.Append %v", l)

	w.Lock()
	defer w.Unlock()

	r, err := w.generateRawRecord(l)
	if err != nil {
		cb(err)
	}
	r.cb = cb

	w.recordCh <- r
}

func (w *Writer) generateRawRecord(l *LogRecord) (rawRecord, error) {
	l.Seq = w.nextSeq
	w.nextSeq++

	w.buf.Reset()
	if err := gob.NewEncoder(w.buf).Encode(l); err != nil {
		return rawRecord{}, err
	}
	data := w.buf.Bytes()
	dataLen := len(data)
	if uint32(dataLen) > MaxRecordBytes {
		return rawRecord{}, fmt.Errorf("log record has encoded size %d that exceeds %d", dataLen, MaxRecordBytes)
	}

	crc := crc32.Checksum(data, crcTable)

	dataCopy := make([]byte, dataLen)
	copy(dataCopy, data)

	r := rawRecord{
		seq:      l.Seq,
		data:     dataCopy,
		checkSum: crc,
	}

	return r, nil
}

func logName(nextSeq int64) string {
	return filepath.Join(config.Config.DBConfig.InternalDirPath, fmt.Sprintf("wal-%d.log", nextSeq))
}

func (w *Writer) rollover(seq int64) error {
	fn := logName(seq)

	utils.Debugf("Rolling over WAL from %v to %v.", w.filename, fn)

	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
		if err := w.f.Sync(); err != nil {
			return err
		}
		if err := w.f.Close(); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	w.filename = fn
	w.f = f
	w.bufWriter = bufio.NewWriter(f)
	w.size = 0

	return nil
}

func (w *Writer) writeLoop() {
	// TODO: error handling
	callbacks := make([]func(error), 0)
Main:
	for {
		callbacks = callbacks[:0]
		// wait for first record
		select {
		case r := <-w.recordCh:
			if err := w.writeRawRecord(r); err != nil {
				r.cb(err)
			} else {
				callbacks = append(callbacks, r.cb)
			}
		case <-w.closeCh:
			break Main
		}

		// write out all remaining records
	L:
		for {
			select {
			case r := <-w.recordCh:
				if err := w.writeRawRecord(r); err != nil {
					r.cb(err)
				} else {
					callbacks = append(callbacks, r.cb)
				}
			default:
				break L
			}
		}

		// sync, then notify.
		utils.Debugf("Notifying %v callbacks", len(callbacks))
		err := w.sync()
		for _, cb := range callbacks {
			cb(err)
		}
	}

	utils.Debugf("wal.writeLoop shutting down")

	if err := w.bufWriter.Flush(); err != nil {
		w.closeResultCh <- err
	}
	w.closeResultCh <- w.f.Close()
}

func (w *Writer) writeRawRecord(r rawRecord) error {
	utils.Debugf("wal writing raw record for seq %v", r.seq)

	if w.size > MaxLogBytes {
		if err := w.rollover(r.seq); err != nil {
			utils.Debugf("Error while attempting to rollover WAL: %v", err)
			return err
		}
	}

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(r.data)))
	binary.LittleEndian.PutUint32(scratch[4:8], r.checkSum)

	if _, err := w.bufWriter.Write(scratch[:]); err != nil {
		return err
	}
	w.size += int64(len(r.data)) + 8

	if _, err := w.bufWriter.Write(r.data); err != nil {
		return err
	}

	return nil
}

func (w *Writer) sync() error {
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	w.closeCh <- struct{}{}
	return <-w.closeResultCh
}
