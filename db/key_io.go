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
	"math"
	"time"

	"github.com/RcrdBrt/gobigdis/ops"
	"github.com/RcrdBrt/gobigdis/sst"
	"github.com/RcrdBrt/gobigdis/utils"
	"github.com/RcrdBrt/gobigdis/wal"
)

func (db *database) set(dbNum int, args [][]byte) error {
	if err := utils.ValidateKey(string(args[0])); err != nil {
		return err
	}

	ch := make(chan error, 1)

	l := &wal.LogRecord{
		Op:        ops.SET,
		DBNum:     dbNum,
		Key:       string(args[0]),
		Value:     args[1],
		Timestamp: time.Now().UnixNano(),
	}

	db.logWriter.Append(l, func(err error) {
		if err != nil {
			ch <- err
			return
		}

		db.Lock()
		db.apply(l)
		db.maybeTriggerFlush()
		db.Unlock()

		ch <- nil
	})

	err := <-ch
	if err != nil {
		return err
	}

	return nil
}

func (d *database) get(dbNum int, args [][]byte) ([]byte, error) {
	key := string(args[0])

	// Acquire local copies of required structures --> release the lock quicky.
	d.RLock()

	ssts := make([]*sst.Reader, len(d.ssts))
	for i, sst := range d.ssts {
		sst.Ref()
		ssts[i] = sst
	}
	defer func() {
		for _, sst := range ssts {
			sst.UnRef()
		}
	}()

	memtable := d.memtable
	imemtable := d.imemtable
	d.RUnlock()

	v, found := memtable.Find(key)
	if found {
		return v, nil
	}
	if imemtable != nil {
		v, found = imemtable.Find(key)
		if found {
			return v, nil
		}
	}

	var value []byte
	valueTs := int64(math.MinInt64)

	for _, s := range ssts {
		v, ts, err := s.Find(key)
		if err == sst.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		if ts > valueTs {
			value = v
			valueTs = ts
		}
	}

	return value, nil
}

func (d *database) del(dbNum int, args [][]byte) (int, error) {
	var deleted int

	return deleted, nil
}
