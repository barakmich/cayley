// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bolt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/barakmich/glog"
	"github.com/boltdb/bolt"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

var (
	boltType    graph.Type
	bufferSize  = 50
	errNotExist = errors.New("quad does not exist")
)

func init() {
	boltType = graph.RegisterIterator("bolt")
}

type Iterator struct {
	uid       uint64
	tags      graph.Tagger
	bucket    []byte
	prefix    []byte
	lset      graph.LinkageSet
	nextCheck graph.LinkageSet
	qs        *QuadStore
	result    *Token
	buffer    [][]byte
	offset    int
	done      bool
	size      int64
	err       error
}

func (it *Iterator) determineBucket() {
	sort.Sort(it.lset)
	switch len(it.lset) {
	case 1:
		it.determineSingleBucket()
		return
	case 2:
		it.determineIndexBucket()
		return
	}
	panic("unsupported number of linkages for Bolt iterator:" + string(len(it.lset)))
}

func (it *Iterator) determineIndexBucket() {
	it.prefix = nil
	it.nextCheck = nil
	d1, d2 := it.lset[0], it.lset[1]
	prefix := fmt.Sprint(d1.Dir.Prefix(), d2.Dir.Prefix())
	switch prefix {
	case "sp":
		it.bucket = spoBucket
	case "so":
		d1, d2 = d2, d1
		it.bucket = ospBucket
	case "sc":
		it.nextCheck = append(it.nextCheck, d2)
		it.determineSingleBucket()
		return
	case "po":
		it.bucket = posBucket
	case "pc":
		d1, d2 = d2, d1
		it.bucket = cpsBucket
	case "oc":
		it.nextCheck = append(it.nextCheck, d2)
		it.determineSingleBucket()
		return
	default:
		panic("unreachable -- no prefix " + prefix)
	}
	it.prefix = append(it.prefix, d1.Value.(*Token).key...)
	it.prefix = append(it.prefix, d2.Value.(*Token).key...)
}

func (it *Iterator) determineSingleBucket() {
	var bucket []byte
	l := it.lset[0]
	switch l.Dir {
	case quad.Subject:
		bucket = spoBucket
	case quad.Predicate:
		bucket = posBucket
	case quad.Object:
		bucket = ospBucket
	case quad.Label:
		bucket = cpsBucket
	default:
		panic("unreachable " + l.Dir.String())
	}
	it.bucket = bucket
	tok := l.Value.(*Token)
	it.prefix = tok.key
}

func NewIterator(lset graph.LinkageSet, qs *QuadStore) *Iterator {

	it := Iterator{
		uid:  iterator.NextUID(),
		qs:   qs,
		lset: lset,
		size: qs.sizeOf(lset[0].Value.(*Token)),
	}
	it.determineBucket()

	return &it
}

func Type() graph.Type { return boltType }

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.buffer = nil
	it.offset = 0
	it.done = false
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	out := NewIterator(it.lset, it.qs)
	out.Tagger().CopyFrom(it)
	return out
}

func (it *Iterator) Close() error {
	it.result = nil
	it.buffer = nil
	it.done = true
	return nil
}

func (it *Iterator) isLiveValue(val []byte) bool {
	var entry IndexEntry
	json.Unmarshal(val, &entry)
	return len(entry.History)%2 != 0
}

func (it *Iterator) matchesNextConstraint(key []byte) bool {
	for _, l := range it.nextCheck {
		offset := PositionOf(Token{it.bucket, key}, l.Dir)
		if len(key) == 0 || !bytes.HasPrefix(key[offset:], l.Value.(*Token).key) {
			return false
		}
	}
	return true
}

func (it *Iterator) Next() bool {
	if it.done {
		return false
	}
	if len(it.buffer) <= it.offset+1 {
		it.offset = 0
		var last []byte
		if it.buffer != nil {
			last = it.buffer[len(it.buffer)-1]
		}
		it.buffer = make([][]byte, 0, bufferSize)
		err := it.qs.db.View(func(tx *bolt.Tx) error {
			i := 0
			b := tx.Bucket(it.bucket)
			cur := b.Cursor()
			if last == nil {
				k, v := cur.Seek(it.prefix)
				if bytes.HasPrefix(k, it.prefix) {
					if it.isLiveValue(v) && it.matchesNextConstraint(k) {
						var out []byte
						out = make([]byte, len(k))
						copy(out, k)
						it.buffer = append(it.buffer, out)
						i++
					}
				} else {
					it.buffer = append(it.buffer, nil)
					return errNotExist
				}
			} else {
				k, _ := cur.Seek(last)
				if !bytes.Equal(k, last) {
					return fmt.Errorf("could not pick up after %v", k)
				}
			}
			for i < bufferSize {
				k, v := cur.Next()
				if k == nil || !bytes.HasPrefix(k, it.prefix) {
					it.buffer = append(it.buffer, nil)
					break
				}
				if !it.isLiveValue(v) || !it.matchesNextConstraint(k) {
					continue
				}
				var out []byte
				out = make([]byte, len(k))
				copy(out, k)
				it.buffer = append(it.buffer, out)
				i++
			}
			return nil
		})
		if err != nil {
			if err != errNotExist {
				glog.Errorf("Error nexting in database: %v", err)
				it.err = err
			}
			it.done = true
			return false
		}
	} else {
		it.offset++
	}
	if it.Result() == nil {
		it.done = true
		return false
	}
	return true
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *Iterator) Result() graph.Value {
	if it.done {
		return nil
	}
	if it.result != nil {
		return it.result
	}
	if it.offset >= len(it.buffer) {
		return nil
	}
	if it.buffer[it.offset] == nil {
		return nil
	}
	return &Token{bucket: it.bucket, key: it.buffer[it.offset]}
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func PositionOf(tok Token, d quad.Direction) int {
	if bytes.Equal(tok.bucket, spoBucket) {
		switch d {
		case quad.Subject:
			return 0
		case quad.Predicate:
			return hashSize
		case quad.Object:
			return 2 * hashSize
		case quad.Label:
			return 3 * hashSize
		}
	}
	if bytes.Equal(tok.bucket, posBucket) {
		switch d {
		case quad.Subject:
			return 2 * hashSize
		case quad.Predicate:
			return 0
		case quad.Object:
			return hashSize
		case quad.Label:
			return 3 * hashSize
		}
	}
	if bytes.Equal(tok.bucket, ospBucket) {
		switch d {
		case quad.Subject:
			return hashSize
		case quad.Predicate:
			return 2 * hashSize
		case quad.Object:
			return 0
		case quad.Label:
			return 3 * hashSize
		}
	}
	if bytes.Equal(tok.bucket, cpsBucket) {
		switch d {
		case quad.Subject:
			return 2 * hashSize
		case quad.Predicate:
			return hashSize
		case quad.Object:
			return 3 * hashSize
		case quad.Label:
			return 0
		}
	}
	panic("unreachable")
}

func (it *Iterator) Contains(v graph.Value) bool {
	val := v.(*Token)
	if bytes.Equal(val.bucket, nodeBucket) {
		return false
	}
	for _, l := range it.lset {
		offset := PositionOf(*val, l.Dir)
		if len(val.key) == 0 || !bytes.HasPrefix(val.key[offset:], l.Value.(*Token).key) {
			// You may ask, why don't we check to see if it's a valid (not deleted) quad
			// again?
			//
			// We've already done that -- in order to get the graph.Value token in the
			// first place, we had to have done the check already; it came from a Next().
			//
			// However, if it ever starts coming from somewhere else, it'll be more
			// efficient to change the interface of the graph.Value for LevelDB to a
			// struct with a flag for isValid, to save another random read.
			return false
		}
	}
	return true
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

func (it *Iterator) Describe() graph.Description {
	return graph.Description{
		UID:       it.UID(),
		Name:      it.qs.NameOf(&Token{it.bucket, it.lset[0].Value.(*Token).key}),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      it.size,
		Direction: it.lset[0].Dir,
	}
}

func (it *Iterator) Type() graph.Type { return boltType }
func (it *Iterator) Sorted() bool     { return false }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     4,
		Size:         s,
	}
}

var _ graph.Nexter = &Iterator{}
