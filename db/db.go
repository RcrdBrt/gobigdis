package db

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/memtable"
	"github.com/RcrdBrt/gobigdis/sst"
	"github.com/RcrdBrt/gobigdis/storage"
	"github.com/RcrdBrt/gobigdis/utils"
)

const blockCacheSize = 1 * 1024 * 1024 * 1024 // 1 GiB

const memtableFlushSize = 20 * 1024 * 1024 // 20 MiB

type database struct {
	sync.RWMutex
	descriptor     *descriptor
	memtable       *memtable.Memtable
	imemtable      *memtable.Memtable
	ssts           []*sst.Reader
	blockCache     *sst.Cache
	manifest       *descriptor
	compactingSsts []string

	dbNum int
}

var DB *database

func Init(configFile string) {
	config.Init(configFile)

	storage.Init()

	var db = &database{
		blockCache: sst.NewCache(blockCacheSize),
		manifest:   loadLatestDescriptor(), // search for most recent MANIFEST file
		dbNum:      0,
	}

	lastAppliedSeqNo := uint64(0)
	for _, sstMeta := range db.manifest.sstMetas {
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
	d.descriptor.sstMetas = append(d.descriptor.sstMetas, newSstMeta)
	if err := d.descriptor.Save(); err != nil {
		log.Fatalf("error saving descriptor while flushing memtable: %v", err)
	}
	d.imemtable = nil
	d.ssts = append(d.ssts, reader)
}
