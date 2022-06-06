package gobitcaskdb

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type DirLogs struct {
	activeLocker  sync.RWMutex
	active        *LogFile
	dir           string
	archive       map[string]*LogFile
	archiveLocker sync.RWMutex
	archiveSize   uint32
}

const AcitveLogfile = "gobitcask.data"

func OpenDirLogs(dir string, archiveSize uint32) (*DirLogs, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	archive := map[string]*LogFile{}
	for _, de := range des {
		if de.IsDir() || !strings.Contains(de.Name(), "archive") {
			continue
		}
		tmp := strings.ReplaceAll(de.Name(), ".data", "")
		id := strings.ReplaceAll(tmp, "archive", "")
		archive[id] = nil
	}

	active, err := NewLogFile(dir+"/"+AcitveLogfile, false, false)
	if err != nil {
		return nil, err
	}

	return &DirLogs{
		active:      active,
		archiveSize: archiveSize,
		dir:         dir,
		archive:     archive,
	}, nil
}

func (df *DirLogs) Offset() (uint32, error) {
	df.activeLocker.RLock()
	defer df.activeLocker.RUnlock()
	size, err := df.active.Size()
	if err != nil {
		return 0, err
	}
	return size, nil
}

func (df *DirLogs) Append(data []byte) (uint32, error) {
	df.activeLocker.RLock()
	offset, err := df.active.Size()
	if err != nil {
		df.activeLocker.RUnlock()
		return 0, err
	}

	err = df.active.Append(data)
	if err != nil {
		df.activeLocker.RUnlock()
		return 0, err
	}
	size, err := df.active.Size()
	if err != nil {
		df.activeLocker.RUnlock()
		return 0, err
	}
	df.activeLocker.RUnlock()

	if size >= df.archiveSize {
		df.activeLocker.Lock()
		defer df.activeLocker.Unlock()
		err = df.active.Close()
		if err != nil {
			return 0, err
		}
		id := fmt.Sprintf("%v", time.Now().UnixNano())
		err = os.Rename(df.dir+"/"+AcitveLogfile, fmt.Sprintf("%s/archive%v.data", df.dir, id))
		if err != nil {
			return 0, err
		}
		active, err := NewLogFile(df.dir+"/"+AcitveLogfile, false, false)
		if err != nil {
			panic(err)
		}
		df.active = active

		df.archiveLocker.Lock()
		df.archive[id] = nil
		df.archiveLocker.Unlock()
	}

	return offset, nil
}

func (df *DirLogs) ReadAt(data []byte, offset int64) error {
	df.activeLocker.RLock()
	defer df.activeLocker.RUnlock()
	err := df.active.ReadAt(data, offset)
	if err != nil {
		return err
	}
	return nil
}

func (df *DirLogs) ReadArchiveAt(id string, data []byte, offset int64) error {
	df.archiveLocker.RLock()
	defer df.archiveLocker.RUnlock()
	if _, ok := df.archive[id]; !ok {
		return errors.New("id not found")
	}
	if df.archive[id] == nil {
		lf, err := NewLogFile(df.dir+"/"+id, false, true)
		if err != nil {
			return err
		}
		df.archive[id] = lf
	}

	err := df.archive[id].ReadAt(data, offset)
	if err != nil {
		return err
	}
	return nil
}
