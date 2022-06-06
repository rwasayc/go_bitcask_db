package gobitcaskdb

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestNormalUse(t *testing.T) {
	db, err := Open(".")
	if err != nil {
		t.Error(err)
		return
	}
	data, exist, err := db.Get([]byte("d"))
	if err != nil {
		t.Error(err)
		return
	}
	if exist {
		t.Error("key is not exist but return true")
		return
	}
	if len(data) > 0 {
		t.Error("key is not exist but return data")
		return
	}
	err = db.Put([]byte("d"), []byte("haha"))
	if err != nil {
		t.Error(err)
		return
	}
	err = db.Put([]byte("d2"), []byte("测试一下"))
	if err != nil {
		t.Error(err)
		return
	}

	data, exist, err = db.Get([]byte("d"))
	if err != nil {
		t.Error(err)
		return
	}
	if !exist {
		t.Error("key is exist but return false")
		return
	}
	if len(data) == 0 {
		t.Error("key is exist but return empty")
		return
	}
}

func BenchmarkGet100000(b *testing.B) {
	b.StopTimer()
	db, err := Open(".")
	if err != nil {
		b.Error(err)
		return
	}
	max := 100000
	for i := 0; i < max; i++ {
		v := []byte(fmt.Sprintf("%v", i))
		db.Put(v, v)
	}
	b.StartTimer()
	b.RunParallel(func(p *testing.PB) {
		target := []byte(fmt.Sprintf("%v", rand.Int()%max))
		for p.Next() {
			db.Get(target)
		}
	})
}

func BenchmarkGet100000Check(b *testing.B) {
	b.StopTimer()
	db, err := Open(".")
	if err != nil {
		b.Error(err)
		return
	}
	max := 100000
	for i := 0; i < max; i++ {
		v := []byte(fmt.Sprintf("%v", i))
		db.Put(v, v)
	}
	b.StartTimer()
	b.RunParallel(func(p *testing.PB) {
		target := []byte(fmt.Sprintf("%v", rand.Int()%max))
		for p.Next() {
			v, ok, err := db.Get(target)
			if v == nil || string(v) != string(target) || !ok || err != nil {
				b.Errorf("get test fail ,target:%v v:%v ok:%v err:%v ", string(target), string(v), ok, err)
				b.FailNow()
			}
		}
	})
}
