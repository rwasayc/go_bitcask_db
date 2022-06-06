package gobitcaskdb

import (
	"io"
	"sync"
)

type GoBitcaskdb struct {
	options *DBOptions
	lock    sync.RWMutex
	indexs  map[string]*Index
	logs    *DirLogs
}

const EntryHeaderSize = 12

func Open(directoryName string, ops ...DBOption) (*GoBitcaskdb, error) {
	options := &DBOptions{}
	for _, op := range ops {
		op(options)
	}
	logs, err := OpenDirLogs(directoryName, 1024*1024*64)
	if err != nil {
		return nil, err
	}

	return &GoBitcaskdb{
		indexs:  map[string]*Index{},
		options: options,
		logs:    logs,
	}, nil
}

func (db *GoBitcaskdb) Merge() error {
	return nil
}

func (db *GoBitcaskdb) Sync() error {
	return nil
}

func (db *GoBitcaskdb) Close() error {
	if db.isClose() {
		panic("db is closed")
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	db.logs = nil
	return nil
}

func (db *GoBitcaskdb) Get(key []byte) ([]byte, bool, error) {
	if db.isClose() {
		panic("db is closed")
	}

	db.lock.RLock()
	defer db.lock.RUnlock()
	if _, ok := db.indexs[string(key)]; !ok {
		return nil, false, nil
	}
	offset := db.indexs[string(key)].Offset
	headerBuf := make([]byte, EntryHeaderSize)
	err := db.logs.ReadAt(headerBuf, int64(offset))
	if err != nil && err != io.EOF {
		return nil, false, err
	}

	entryHeader, err := UnmarshalHeader(headerBuf)
	if err != nil {
		return nil, false, err
	}
	entityBuf := make([]byte, entryHeader.Ksz+entryHeader.Vsz)
	err = db.logs.ReadAt(entityBuf, int64(offset+EntryHeaderSize))
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	entry, err := UnmarshalEntity(entryHeader, entityBuf)
	if err != nil {
		return nil, false, err
	}
	return entry.V, true, nil
}

func (db *GoBitcaskdb) Put(key, val []byte) error {
	if db.isClose() {
		panic("db is closed")
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	data, err := Marshal(NewEntry(key, val))
	if err != nil {
		return err
	}
	offset, err := db.logs.Append(data)
	if err != nil {
		return err
	}
	db.indexs[string(key)] = &Index{Offset: offset}

	return nil
}

func (db *GoBitcaskdb) Delete(key []byte) error {
	return db.Put(key, nil)
}

func (db *GoBitcaskdb) ListKeys() ([][]byte, error) {
	if db.isClose() {
		panic("db is closed")
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	return nil, nil
}

func (db *GoBitcaskdb) isClose() bool {
	return db.logs == nil
}
