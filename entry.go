package gobitcaskdb

import (
	"encoding/binary"
	"time"
)

type Entry struct {
	H *EntryHeader
	K []byte
	V []byte
}
type EntryHeader struct {
	Tstamp uint32
	Ksz    uint32
	Vsz    uint32
}

func NewEntry(K, V []byte) *Entry {
	return &Entry{
		H: &EntryHeader{
			Tstamp: uint32(time.Now().UnixNano()),
			Ksz:    uint32(len(K)),
			Vsz:    uint32(len(V)),
		},
		K: K,
		V: V,
	}
}

func UnmarshalHeader(buf []byte) (*EntryHeader, error) {
	if len(buf) < EntryHeaderSize {
		return nil, ErrInvalidHeaderBuffer
	}
	ts := binary.BigEndian.Uint32(buf[0:4])
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])

	return &EntryHeader{
		Tstamp: ts,
		Ksz:    ks,
		Vsz:    vs,
	}, nil
}

func UnmarshalEntity(header *EntryHeader, buf []byte) (*Entry, error) {
	if uint32(len(buf)) != (header.Ksz + header.Vsz) {
		return nil, ErrInvalidEntityBuffer
	}
	return &Entry{
		H: header,
		K: buf[:header.Ksz],
		V: buf[header.Ksz:],
	}, nil
}

func Marshal(e *Entry) ([]byte, error) {
	buf := make([]byte, EntryHeaderSize+len(e.K)+len(e.V))
	binary.BigEndian.PutUint32(buf[0:4], e.H.Tstamp)
	binary.BigEndian.PutUint32(buf[4:8], e.H.Ksz)
	binary.BigEndian.PutUint32(buf[8:12], e.H.Vsz)
	copy(buf[EntryHeaderSize:EntryHeaderSize+e.H.Ksz], e.K)
	copy(buf[EntryHeaderSize+e.H.Ksz:], e.V)
	return buf, nil
}
