package db

import (
	"log"
	"os"

	"github.com/RcrdBrt/gobigdis/ops"
	"github.com/RcrdBrt/gobigdis/storage"
	"github.com/RcrdBrt/gobigdis/utils"
	"github.com/RcrdBrt/gobigdis/wal"
)

func (db *database) recoverLog(lastApplied int64) (int64, error) {
	sc, err := wal.NewScanner()
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		log.Fatal(err)
	}

	n := int64(0)
	applied := int64(0)
	seqNo := lastApplied

	for sc.Scan() {
		record := sc.Record()
		n++

		utils.Debugf("recovering log record %v", record)

		if record.Seq <= seqNo {
			// already seen, continue
			continue
		}

		applied++

		seqNo = record.Seq

		db.apply(record)
	}

	db.maybeFlush()

	utils.Debugf("scanned %d records, applied %d", n, applied)

	if seqNo == -1 {
		// TODO: it's possible that if we truncate the log and don't have any new mutations
		// we won't get a sequence number, even if we can recover it from the file metadata.
		log.Fatal("seqNo was not recovered")
	}

	return seqNo, sc.Err()
}

func (db *database) apply(record *wal.LogRecord) {
	switch record.Op {
	case ops.SET:
		storage.Set(record.DBNum, [][]byte{record.Key, record.Value})
	case ops.GET:
		storage.Get(record.DBNum, [][]byte{record.Key})
	case ops.DEL:
		storage.Del(record.DBNum, [][]byte{record.Key})
	}
}

func (db *database) maybeFlush() {
	if db.memtable.SizeBytes() > memtableFlushSize {
		db.swapMemtableLocked()
		go db.flushIMemtable()
	}
}