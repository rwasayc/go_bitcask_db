package gobitcaskdb

import (
	"errors"
	"os"
	"sync"
)

type LogFile struct {
	f        *os.File
	locker   sync.RWMutex
	readOnly bool
	close    bool
}

func NewLogFile(filepath string, sync, readOnly bool) (*LogFile, error) {
	flag := os.O_CREATE | os.O_APPEND
	if sync {
		flag = flag | os.O_SYNC
	}
	if readOnly {
		flag = flag | os.O_RDONLY
	} else {
		flag = flag | os.O_RDWR
	}

	f, err := os.OpenFile(filepath, flag, 0644)
	if err != nil {
		return nil, err
	}

	return &LogFile{
		f:        f,
		readOnly: readOnly,
	}, nil
}

func (lf *LogFile) Append(data []byte) error {
	if lf.readOnly {
		return errors.New("read only")
	}
	lf.locker.Lock()
	defer lf.locker.Unlock()
	if lf.close {
		return errors.New("closed")
	}

	_, err := lf.f.Write(data)
	return err
}

func (lf *LogFile) ReadAt(buf []byte, offset int64) error {
	lf.locker.RLock()
	defer lf.locker.RUnlock()
	if lf.close {
		return errors.New("closed")
	}
	_, err := lf.f.ReadAt(buf, offset)
	return err
}

func (lf *LogFile) Sync() error {
	lf.locker.Lock()
	defer lf.locker.Unlock()
	if lf.close {
		return errors.New("closed")
	}

	return lf.f.Sync()
}

func (lf *LogFile) Close() error {
	lf.locker.Lock()
	defer lf.locker.Unlock()
	lf.close = true
	return lf.f.Close()
}

func (lf *LogFile) Size() (uint32, error) {
	lf.locker.Lock()
	defer lf.locker.Unlock()
	if lf.close {
		return 0, ErrFileClosed
	}

	stat, err := lf.f.Stat()
	if err != nil {
		return 0, err
	}
	return uint32(stat.Size()), nil
}
