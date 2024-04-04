package pebble

import (
	"time"

	"github.com/cockroachdb/pebble/internal/manifest"
)

type levelFileMetadata struct {
	*manifest.FileMetadata
	level int
}

type compactionPickerUniversal struct {
	opts                             *Options
	vers                             *version
	filesMarkedForPeriodicCompaction []levelFileMetadata
}

func (p *compactionPickerUniversal) pickAuto(env compactionEnv) (pc *pickedCompaction) {
	// 1. Compute Files for Universal Compaction
	// 2. Pick and return periodic compaction

	p.computeFilesForUniversalCompaction()

	return nil

}

func (p *compactionPickerUniversal) computeFilesForUniversalCompaction() {
	// [Q] Add this to options
	periodicCompactionsSeconds := int64(1)

	// [Q] Should we compute compaction scores?

	maxOutputLevel := numLevels - 1
	p.computeFilesMarkedForPeriodicCompaction(periodicCompactionsSeconds, maxOutputLevel)

}

func (p *compactionPickerUniversal) computeFilesMarkedForPeriodicCompaction(periodicCompactionsSeconds int64, lastLevel int) {
	// Clear the current slice of files
	p.filesMarkedForPeriodicCompaction = []levelFileMetadata{}

	if periodicCompactionsSeconds == 0 {
		return
	}

	currentTime := time.Now().Unix()
	if periodicCompactionsSeconds > currentTime {
		return
	}

	allowedTimeLimit := currentTime - periodicCompactionsSeconds
	for i := 0; i <= lastLevel; i++ {
		iter := p.vers.Levels[i].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.CreationTime < allowedTimeLimit {
				fileWithLevel := levelFileMetadata{
					FileMetadata: f,
					level:        i,
				}
				p.filesMarkedForPeriodicCompaction = append(p.filesMarkedForPeriodicCompaction, fileWithLevel)
			}
		}

	}

}

type sortedRunInfo struct {
	level int

	// `file` Will be nil for level > 0. For level = 0, the sorted run is
	// for this file.
	file *manifest.FileMetadata

	// For level > 0, `size` and `compensatedFileSize` are sum of sizes all
	// files in the level.
	size                uint64
	compensatedFileSize uint64

	// `beingCompacted` should be the same for all files
	// in a non-zero level. Use the value here.
	beingCompacted bool
}

func calculateSortedRuns(vers manifest.Version, lastLevel int) []sortedRunInfo {
	ret := make([]sortedRunInfo, 0)

	iter := vers.Levels[0].Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		sortedRun := sortedRunInfo{
			level:               0,
			file:                f,
			size:                f.Size,
			compensatedFileSize: fileCompensation(f),
			beingCompacted:      f.IsCompacting(),
		}
		ret = append(ret, sortedRun)

	}

	for i := 1; i <= lastLevel; i++ {
		lm := vers.Levels[i]
		totalCompensatedSize := levelCompensatedSize(lm)
		beingCompacted := false

		iter = vers.Levels[i].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.IsCompacting() {
				beingCompacted = true
			}
		}
		if totalCompensatedSize > 0 {
			sortedRun := sortedRunInfo{
				level:               i,
				file:                nil,
				size:                lm.Size(),
				compensatedFileSize: totalCompensatedSize,
				beingCompacted:      beingCompacted,
			}
			ret = append(ret, sortedRun)
		}

	}

	return ret
}
