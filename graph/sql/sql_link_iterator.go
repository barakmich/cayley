// Copyright 2015 The Cayley Authors. All rights reserved.
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
	"sync/atomic"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

var sqlLinkType graph.Type
var sqlTableID uint64

func init() {
	sqlLinkType = graph.RegisterIterator("sqllink")
	sqlNodeType = graph.RegisterIterator("sqlnode")
	atomic.StoreUint64(&sqlTableID, 1)
}

func newTableName() string {
	id := atomic.AddUint64(&sqlTableID, 1)
	return fmt.Sprintf("t_%d", id)
}

type constraint struct {
	dir  quad.Direction
	vals []string
}

type tagDir struct {
	tag string
	dir quad.Direction
}

type sqlItDir struct {
	dir quad.Direction
	it  sqlIterator
}

type sqlIterator interface {
	sqlClone() sqlIterator
}

type SQLLinkIterator struct {
	uid    uint64
	qs     *QuadStore
	tagger graph.Tagger
	err    error

	cursor      *sql.Rows
	nodeIts     []sqlItDir
	constraints []constraint
	tableName   string
	size        int64

	result      map[string]string
	resultIndex int
	resultList  [][]string
	resultNext  [][]string
	cols        []string
	resultQuad  quad.Quad
}

func NewSQLLinkIterator(qs *QuadStore, d quad.Direction, val string) *SQLLinkIterator {
	l := &SQLLinkIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		constraints: []constraint{
			constraint{
				dir:  d,
				vals: []string{val},
			},
		},
		size: 0,
	}
	return l
}

func (l *SQLLinkIterator) sqlClone() sqlIterator {
	return l.Clone().(*SQLLinkIterator)
}

func (l *SQLLinkIterator) Clone() graph.Iterator {
	m := &SQLLinkIterator{
		uid:       iterator.NextUID(),
		qs:        l.qs,
		tableName: l.tableName,
		size:      l.size,
	}
	for _, i := range l.nodeIts {
		m.nodeIts = append(m.nodeIts, sqlItDir{
			dir: i.dir,
			it:  i.it.sqlClone(),
		})
	}
	copy(l.constraints, m.constraints)
	m.tagger.CopyFrom(l)
	return m
}

func (l *SQLLinkIterator) UID() uint64 {
	return l.uid
}

func (l *SQLLinkIterator) Reset() {
	l.err = nil
	l.Close()
}

func (l *SQLLinkIterator) Err() error {
	return l.err
}

func (l *SQLLinkIterator) Close() error {
	if l.cursor != nil {
		err := l.cursor.Close()
		if err != nil {
			return err
		}
		l.cursor = nil
	}
	return nil
}

func (l *SQLLinkIterator) Tagger() *graph.Tagger {
	return &l.tagger
}

func (l *SQLLinkIterator) Result() graph.Value {
	return l.resultQuad
}

func (l *SQLLinkIterator) TagResults(dst map[string]graph.Value) {
	for tag, value := range l.result {
		if tag == "__execd" {
			for _, tag := range l.tagger.Tags() {
				dst[tag] = value
			}
			continue
		}
		dst[tag] = value
	}

	for tag, value := range l.tagger.Fixed() {
		dst[tag] = value
	}
}

func (l *SQLLinkIterator) SubIterators() []graph.Iterator {
	// TODO(barakmich): SQL Subiterators shouldn't count? If it makes sense,
	// there's no reason not to expose them though.
	return nil
}

func (l *SQLLinkIterator) Sorted() bool                     { return false }
func (l *SQLLinkIterator) Optimize() (graph.Iterator, bool) { return l, false }

func (l *SQLLinkIterator) Size() (int64, bool) {
	if l.size != 0 {
		return l.size, true
	}
	if len(l.constraints) > 0 {
		l.size = l.qs.sizeForIterator(false, l.constraints[0].dir, l.constraints[0].vals[0])
	} else {
		return l.qs.Size(), false
	}
	return l.size, true
}

func (l *SQLLinkIterator) Describe() graph.Description {
	size, _ := l.Size()
	return graph.Description{
		UID:  l.UID(),
		Name: fmt.Sprintf("SQL_LINK_QUERY: %#v", l),
		Type: l.Type(),
		Size: size,
	}
}

func (l *SQLLinkIterator) Stats() graph.IteratorStats {
	size, _ := l.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}

func (l *SQLLinkIterator) Type() graph.Type {
	return sqlLinkType
}

func (l *SQLLinkIterator) Contains(v graph.Value) bool {
	// STUB
	return false
}

func (l *SQLLinkIterator) NextPath() bool {
	l.resultIndex += 1
	if l.resultIndex >= len(l.resultList) {
		return false
	}
	l.buildResult(l.resultIndex)
	return true
}

func (l *SQLLinkIterator) buildResult(i int) {
	container := l.resultList[i]
	var q quad.Quad
	q.Subject = container[0]
	q.Predicate = container[1]
	q.Object = container[2]
	q.Label = container[3]
	l.resultQuad = q
	l.result = make(map[string]string)
	for i, c := range l.cols[4:] {
		l.result[c] = container[i+4]
	}
}

type SQLAllIterator struct {
	// TBD
}
