package db

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/memtable"
	"github.com/RcrdBrt/gobigdis/sst"
	"github.com/RcrdBrt/gobigdis/storage"
)

const blockCacheSize = 1 * 1024 * 1024 * 1024 // 1 GiB

type database struct {
	sync.RWMutex
	memtable       *memtable.Memtable
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

	lastAppliedSeqNo := int64(0)
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

	// TODO: WAL

	DB = db
}

func (db *database) StartServer() {
	fmt.Printf("GoBigdis TCP redis server is listening on %s:%d\n", config.Config.ServerConfig.Host, config.Config.ServerConfig.Port)
	startRedisTCPServer()
}
