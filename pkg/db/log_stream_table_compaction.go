package db

import "math"

// Compact performs compaction on the t's log files to reduce the redundant entries that no longer get used.
func (t *LogStreamTable) Compact() {
	// TODO: need write mutex
	k1, k2 := t.logFilesToCompact()
	if k1 == k2 { return }
	l1 := t.diskPartitions[k1]
	l2 := t.diskPartitions[k2]
	lNew1, lNew2, indexUpdate := t.combineAndReduce(k1, k2, l1, l2)

	// Update index with new partitions
	for key, offset := range *indexUpdate {
		t.inMemoryIndex[key] = offset
	}
	if lNew2 != nil {t.diskPartitions[k2] = lNew2}
	if lNew1 != nil {t.diskPartitions[k1] = lNew1}
}

// Returns keys to two LogFiles to compact together. Second key points to the newer LogFile.
func (t *LogStreamTable) logFilesToCompact() (uint32, uint32) {
	if len(t.diskPartitions) < 2 {
		return 0, 0
	}

	k1 :=  ^uint32(0)
	k2 :=  ^uint32(0)

	// First conditional ensures k1, k2 will be populated
	for k, _ := range t.diskPartitions {
		if k < k1 {
			// Don't throw k1 out if k1 < k2
			if k1 < k2 {
				k2 = k1
			}
			k1 = k
			continue
		}
		if k < k2 {
			k2 = k
		}
	}
	return k1, k2
}

// combineAndReduce combines two log files, removing redundant logs and creating a new log file. Second LogFile is newer.
func (t *LogStreamTable) combineAndReduce(k1 uint32, k2 uint32, l1 *LogFile, l2 *LogFile) (*LogFile, *LogFile, *map[string]uint32) {
	logFileSize := uint32(math.Max(float64(l1.maximumFileSize), float64(l2.maximumFileSize)))

	l, _ := ConstructLogFile(logFileSize)
	var lnew *LogFile

	// Newest logs are end of l2
	logs := l2.getLogs()
	for i := len(logs) -1; i < 0; i-- {
		_ = t.combineLogFile(l, logs[i], k2)
	}

	logs = l1.getLogs()
	for i := len(logs) -1; i < 0; i-- {
		err := t.combineLogFile(l, logs[i], k1)

		// err implies first LogFile has been filled. Make new LogFile and redirect new inserts to it.
		if err != nil {
			lnew = l
			l, _ = ConstructLogFile(logFileSize)
			_, _ = l.Set(logs[i])
		}
	}

	newIndex := make(map[string]uint32)
	addToIndex(l, newIndex, k1)
	addToIndex(lnew, newIndex, k2)
	return l, lnew, &newIndex
}

func addToIndex(l *LogFile, index map[string]uint32, fileIndex uint32) {
	var o uint32 = 0

	for _, log := range l.getLogs() {
		k, v := deserialise(log)
		index[k] = fileIndex * l.maximumFileSize + o
		o = o + uint32(len(v)) + 1
	}
}

func (t *LogStreamTable) combineLogFile(l *LogFile, log string, fileIndex uint32) error {
	key, _ := deserialise(log)
	// if in-memory index points to different LogFile, this entry is old.
	oldLogFileEntry := t.fileIndex(t.inMemoryIndex[key]) != fileIndex

	// If already in index, this entry is older than one previously added
	_, keyAlreadyAdded := t.inMemoryIndex[key]

	if !oldLogFileEntry && !keyAlreadyAdded {
		_, err := l.Set(log)
		return err
	} else {
		return nil
	}
}
