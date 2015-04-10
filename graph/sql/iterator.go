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

package sql

import (
	"database/sql"
	"fmt"

	"github.com/barakmich/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

type Iterator struct {
	uid    uint64
	tags   graph.Tagger
	qs     *QuadStore
	dir    quad.Direction
	val    graph.Value
	size   int64
	isAll  bool
	table  string
	cursor *sql.Rows
	result graph.Value
}

func (it *Iterator) makeCursor() {
	var cursor *db.Rows
	if it.cursor != nil {
		it.cursor.Close()
	}
	if it.isAll {
		if it.table == "quads" {
			cursor, err := qs.db.QueryRows(`SELECT subject, predicate, object, label FROM quads;`)
			if err != nil {
				glog.Errorln("Couldn't get cursor from SQL database: %v", err)
				cursor = nil
			}
		} else {
			cursor, err := qs.db.QueryRows(`SELECT node FROM nodes;`)
			if err != nil {
				glog.Errorln("Couldn't get cursor from SQL database: %v", err)
				cursor = nil
			}
		}
	} else {
		cursor, err := qs.db.QueryRows(`
		SELECT subject, predicate, object, label FROM quads WHERE ? = ?;`, d.String(), val.(string))
		if err != nil {
			glog.Errorln("Couldn't get cursor from SQL database: %v", err)
			cursor = nil
		}
	}
	it.cursor = cursor
}

func NewIterator(qs *QuadStore, d quad.Direction, val graph.Value) *Iterator {
	var size int64
	err := qs.db.QueryRow(`SELECT count(*) FROM quads WHERE ? = ?;`, d.String(), val.(string)).Scan(&size)
	if err != nil {
		glog.Errorln("Error getting size from SQL database: %v", err)
		return nil
	}
	it := &Iterator{
		uid:    iterator.NextUID(),
		qs:     qs,
		dir:    d,
		cursor: cursor,
		size:   size,
		val:    val,
		table:  "quads",
		isAll:  false,
	}
	it.makeCursor()
	return it
}

func NewAllIterator(qs *QuadStore, table string) *Iterator {
	var size int64
	err := qs.db.QueryRow(`SELECT count(*) FROM ?`, table).Scan(&size)
	if err != nil {
		glog.Errorln("Error getting size for all iterator from SQL database: %v", err)
		return nil
	}
	var cursor *sql.Rows
	it := &Iterator{
		uid:    iterator.NextUID(),
		qs:     qs,
		dir:    d,
		cursor: cursor,
		size:   size,
		table:  table,
		isAll:  true,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.Close()
	it.makeCursor()
}

func (it *Iterator) Close() {
	it.cursor.Close()
	it.cursor = nil
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
	var m *Iterator
	if it.isAll {
		m = NewAllIterator(it.qs, it.table)
	} else {
		m = NewIterator(it.qs, it.dir, it.val)
	}
	m.tags.CopyFrom(it)
	return m
}

func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Next() bool {
	var result quad.Quad
	if !it.cursor.Next() {
		err := it.cursor.Err()
		if err != nil {
			glog.Errorf("Cursor error in SQL: %v", err)
		}
		it.cursor.Close()
		it.cursor = nil
		return false
	}
	if table == "nodes" {
		var node string
		err := it.cursor.Scan(&node)
		if err != nil {
			glog.Errorf("Error nexting node iterator: %v", err)
			return false
		}
		it.result = node
		return true
	}
	var q quad.Quad
	err := it.iter.Next(&q.Subject, &q.Predicate, &q.Object, &q.Label)
	if err != nil {
		glog.Errorf("Error nexting link iterator: %v", err)
		return false
	}
	it.result = q
	return true
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	q := v.(quad.Quad)
	if q.Get(it.dir) == it.val.(string) {
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) NextPath() bool {
	return false
}

var sqlType graph.Type

func init() {
	sqlType = graph.RegisterIterator("sql")
}

func Type() graph.Type { return sqlType }

func (it *Iterator) Type() graph.Type {
	if it.isAll {
		return graph.All
	}
	return mongoType
}

func (it *Iterator) Sorted() bool                     { return true }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: fmt.Sprintf("%s/%s", it.val.(string), it.dir),
		Type: it.Type(),
		Size: size,
	}
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}
