package db

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/sst"
	"github.com/RcrdBrt/gobigdis/utils"
)

// compactor monitors the number of SSTs, and triggers compaction when necessary.
// Currently the scheme is a very simple one - if there are more than 8 SSTs then compaction
// of all the SSTs is triggered.
func (db *database) sstCompactor() {
	for range time.Tick(time.Second * 10) {
		var toCompact []*sst.Reader
		db.RLock()
		if len(db.ssts) > 8 {
			toCompact = db.ssts
		}
		db.RUnlock()

		if len(toCompact) > 0 {
			db._sstCompact(toCompact)
		}
	}
}

// compact compacts ssts into a single SST and modifies the descriptor as appropriate.
func (db *database) _sstCompact(ssts []*sst.Reader) {
	start := time.Now().UnixNano()
	fullFilePath := filepath.Join(config.Config.DBConfig.InternalDirPath, fmt.Sprintf("%020d.sst", start))

	db.Lock()
	db.compactingSsts = append(db.compactingSsts, fullFilePath)
	db.Unlock()

	utils.Debugf("Compacting %d SSTs into %s", len(ssts), fullFilePath)

	iters := make([]Iter, len(ssts))
	for i, sst := range ssts {
		iter, err := sst.NewIter()
		if err != nil {
			panic(err)
		}

		iters[i] = iter
	}

	mIter, err := newMergingIter(iters)
	if err != nil {
		panic(err)
	}
	defer mIter.Close()

	writer, err := sst.NewWriter(fullFilePath)
	if err != nil {
		panic(err)
	}

	for {
		hasNext, err := mIter.Next()
		if err != nil {
			panic(err)
		}

		if !hasNext {
			break
		}

		writer.Append(mIter.Key(), mIter.Timestamp(), mIter.Value())
	}

	if err := writer.Close(); err != nil {
		panic(err)
	}

	// TODO: Daniel Chia says a fsync is probably needed here

	utils.Debugf("Compaction of %d SSTs into %s took %d ms", len(ssts), fullFilePath, (time.Now().UnixNano()-start)/1000000)

	filenames := make(map[string]bool)
	for _, sst := range ssts {
		filenames[sst.Filename()] = true
	}

	reader, err := sst.NewReader(fullFilePath, db.blockCache)
	if err != nil {
		panic(err)
	}

	db.Lock()
	defer db.Unlock()

	var newMetas []sst.SstMeta
	maxApplied := uint64(0)
	for _, sstMeta := range db.manifest.sstMetas {
		if filenames[filepath.Join(config.Config.DBConfig.InternalDirPath, sstMeta.Filename)] {
			if sstMeta.AppliedUntil > maxApplied {
				maxApplied = sstMeta.AppliedUntil
			}
			continue
		}

		newMetas = append(newMetas, sstMeta)
	}

	// add new sstMeta to the list of new metas
	newMetas = append(newMetas, sst.SstMeta{
		Filename:     fullFilePath,
		AppliedUntil: maxApplied,
	})

	db.manifest.sstMetas = newMetas
	if err := db.manifest.Save(); err != nil {
		panic(err)
	}

	utils.Debugf("descriptor after compaction: %+v", db.manifest)

	newCompactingFiles := db.compactingSsts[:0]
	for _, filename := range db.compactingSsts {
		if filename != fullFilePath {
			newCompactingFiles = append(newCompactingFiles, filename)
		}
	}
	db.compactingSsts = newCompactingFiles

	var newSsts []*sst.Reader
	for _, sst := range db.ssts {
		if filenames[sst.Filename()] {
			sst.UnRef()
			continue
		}

		newSsts = append(newSsts, sst)
	}
	newSsts = append(newSsts, reader)
	db.ssts = newSsts
}
