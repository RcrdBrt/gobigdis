package db

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/memtable"
	"github.com/RcrdBrt/gobigdis/sst"
	"github.com/RcrdBrt/gobigdis/storage"
	"github.com/RcrdBrt/gobigdis/utils"
	"github.com/RcrdBrt/gobigdis/wal"
)

type database struct {
	sync.RWMutex
	manifest       *descriptor
	memtable       *memtable.Memtable
	imemtable      *memtable.Memtable
	ssts           []*sst.Reader
	blockCache     *sst.Cache
	compactingSsts []string
	logWriter      *wal.Writer

	dbNum int
}

var DB *database

func Init(configFile string) {
	config.Init(configFile)

	storage.Init()

	var db = &database{
		blockCache: sst.NewCache(),
		manifest:   loadLatestDescriptor(), // search for most recent MANIFEST file
		dbNum:      0,
	}

	lastAppliedSeqNo := int64(0)
	for _, sstMeta := range db.manifest.SstMetas {
		if sstMeta.AppliedUntil > lastAppliedSeqNo {
			lastAppliedSeqNo = sstMeta.AppliedUntil
		}

		fp := filepath.Join(config.Config.DBConfig.InternalDirPath, sstMeta.Filename)

		sstReader, err := sst.NewReader(fp, db.blockCache)
		if err != nil {
			log.Fatal(err)
		}
		db.ssts = append(db.ssts, sstReader)
	}

	db.memtable = memtable.New(lastAppliedSeqNo)

	nextSeq, err := db.recoverLog(lastAppliedSeqNo)
	if err != nil {
		log.Fatal(err)
	}

	logWriter, err := wal.NewWriter(nextSeq)
	if err != nil {
		log.Fatal(err)
	}
	db.logWriter = logWriter

	go db.cleanUnusedFiles()
	go db.sstCompactor()

	DB = db
}

func (db *database) StartServer() {
	fmt.Printf("GoBigdis TCP redis server is listening on %s:%d\n", config.Config.ServerConfig.Host, config.Config.ServerConfig.Port)
	startRedisTCPServer()
}

func (d *database) swapMemtableLocked() {
	m := d.memtable
	d.imemtable = m
	d.memtable = memtable.New(m.SequenceUpper())
}

func (d *database) flushIMemtable() {
	if d.imemtable == nil {
		log.Fatalf("flushIMemtable called when imemtable == nil")
	}

	m := d.imemtable
	ts := time.Now().UnixNano()
	fn := fmt.Sprintf("%020d.sst", ts)
	fullFn := filepath.Join(config.Config.DBConfig.InternalDirPath, fn)

	utils.Debugf("flushing memtable of size %v to %v", m.SizeBytes(), fullFn)

	writer, err := sst.NewWriter(fullFn)
	if err != nil {
		log.Fatalf("error opening SST while flushing memtable: %v", err)
	}
	it := m.NewIterator()
	for it.Next() {
		if err := writer.Append(it.Key(), it.Timestamp(), it.Value()); err != nil {
			log.Fatalf("error appending SST while flushing memtable: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		log.Fatalf("error closing SST while flushing memtable: %v", err)
	}

	utils.Debugf("flush completed for %v", fullFn)
	// TODO: need to indicate that earlier log entries no longer needed.

	reader, err := sst.NewReader(fullFn, d.blockCache)
	if err != nil {
		log.Fatalf("error opening SST that was just flushed: %v", err)
	}
	newSstMeta := sst.SstMeta{Filename: fn, AppliedUntil: m.SequenceUpper()}

	d.Lock()
	defer d.Unlock()
	// Holding the db lock during descriptor save here - potentially slow.
	// Most DB operations (including mutations) probably only need a read lock on descriptor
	// so perhaps we need to finer-grained locking around the descriptor.
	d.manifest.SstMetas = append(d.manifest.SstMetas, newSstMeta)
	if err := d.manifest.Save(); err != nil {
		log.Fatalf("error saving descriptor while flushing memtable: %v", err)
	}
	d.imemtable = nil
	d.ssts = append(d.ssts, reader)
}

func (d *database) cleanUnusedFiles() {
	for range time.Tick(30 * time.Second) {
		var maxApplied int64
		d.RLock()
		for _, sst := range d.manifest.SstMetas {
			if sst.AppliedUntil > maxApplied {
				maxApplied = sst.AppliedUntil
			}
		}
		d.RUnlock()
		wal.CleanUnusedFiles(maxApplied)

		liveSstFiles := make(map[string]bool)
		d.RLock()
		for _, sst := range d.ssts {
			liveSstFiles[sst.Filename()] = true
		}
		for _, f := range d.compactingSsts {
			liveSstFiles[f] = true
		}
		d.RUnlock()
		utils.Debugf("Live SSTs are %v", liveSstFiles)

		sstFiles, err := sst.GetSstFiles()
		if err != nil {
			utils.Debugf("error while scanning SST dir for cleanup: %v", err)
			continue
		}

		var cleaned int
		for _, fn := range sstFiles {
			fullFn := filepath.Join(config.Config.DBConfig.InternalDirPath, fn)
			if !liveSstFiles[fullFn] {
				utils.Debugf("Deleting unused SST %v", fullFn)
				if err := os.Remove(fullFn); err != nil {
					utils.Debugf("Error while removing unused SST file %v: %v", fullFn, err)
				} else {
					cleaned++
				}
			}
		}
		if cleaned > 0 {
			utils.Debugf("Cleaned %v unused SST files", cleaned)
		}
	}
}

func (db *database) maybeTriggerFlush() {
	utils.Debugf("maybeTriggerFlush(), memtable size is %v, configured trigger size is %d\n", db.memtable.SizeBytes(), config.Config.DBConfig.MemtableFlushSize)
	if db.memtable.SizeBytes() > config.Config.DBConfig.MemtableFlushSize && db.imemtable == nil {
		db.swapMemtableLocked()
		go db.flushIMemtable()
	}
}
