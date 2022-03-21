package wal

import (
	"os"
	"path/filepath"

	"github.com/RcrdBrt/gobigdis/utils"
)

// CleanUnusedFiles cleans unused log files, i.e those that have already been applied.
func CleanUnusedFiles(dirname string, appliedUntil int64) {
	parsedNames, err := listLogFiles(dirname)
	if err != nil {
		utils.Debugf("error listing log files: %v", err)
		return
	}

	cleaned := 0
	for i, pn := range parsedNames {
		if pn.seqNo < appliedUntil && i > 0 {
			// can delete *previous* logfile, which spans
			// [parsedNames[i-1].seqNo, parsedNames[i].seqNo)
			fullFn := filepath.Join(dirname, parsedNames[i-1].name)
			utils.Debugf("deleting unused log file %v", fullFn)

			if err := os.Remove(fullFn); err != nil {
				utils.Debugf("error while removing unused logfile %v: %v", fullFn, err)
			} else {
				cleaned++
			}
		}
	}

	if cleaned > 0 {
		utils.Debugf("cleaned %v unused log files", cleaned)
	}
}
