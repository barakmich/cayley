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

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

var sqlNodeType graph.Type

func init() {
	sqlNodeType = graph.RegisterIterator("sqlnode")
}

type SQLNodeIterator struct {
	uid    uint64
	qs     *QuadStore
	tagger graph.Tagger
	err    error

	cursor  *sql.Rows
	linkIts []sqlItDir
	size    int64
	tagdirs []tagDir

	result      map[string]string
	resultIndex int
	resultList  [][]string
	resultNext  [][]string
	cols        []string
}

func (n *SQLNodeIterator) sqlClone() sqlIterator {
	return n.Clone().(*SQLNodeIterator)
}

func (n *SQLNodeIterator) Clone() graph.Iterator {
	m := &SQLNodeIterator{
		uid:  iterator.NextUID(),
		qs:   n.qs,
		size: n.size,
	}
	for _, i := range n.linkIts {
		m.linkIts = append(m.linkIts, sqlItDir{
			dir: i.dir,
			it:  i.it.sqlClone(),
		})
	}
	copy(n.tagdirs, m.tagdirs)
	m.tagger.CopyFrom(n)
	return m
}

func (n *SQLNodeIterator) UID() uint64 {
	return n.uid
}

func (n *SQLNodeIterator) Reset() {
	n.err = nil
	n.Close()
}

func (n *SQLNodeIterator) Err() error {
	return n.err
}

func (n *SQLNodeIterator) Close() error {
	if n.cursor != nil {
		err := n.cursor.Close()
		if err != nil {
			return err
		}
		n.cursor = nil
	}
	return nil
}

func (n *SQLNodeIterator) Tagger() *graph.Tagger {
	return &n.tagger
}

func (n *SQLNodeIterator) Result() graph.Value {
	return n.result["__execd"]
}

func (n *SQLNodeIterator) TagResults(dst map[string]graph.Value) {
	for tag, value := range n.result {
		if tag == "__execd" {
			for _, tag := range n.tagger.Tags() {
				dst[tag] = value
			}
			continue
		}
		dst[tag] = value
	}

	for tag, value := range n.tagger.Fixed() {
		dst[tag] = value
	}
}

func (n *SQLNodeIterator) Type() graph.Type {
	return sqlNodeType
}

func (n *SQLNodeIterator) SubIterators() []graph.Iterator {
	// TODO(barakmich): SQL Subiterators shouldn't count? If it makes sense,
	// there's no reason not to expose them though.
	return nil
}

func (n *SQLNodeIterator) Sorted() bool                     { return false }
func (n *SQLNodeIterator) Optimize() (graph.Iterator, bool) { return n, false }

func (n *SQLNodeIterator) Size() (int64, bool) {
	return n.qs.Size() / int64(len(n.linkIts)+1), true
}

func (n *SQLNodeIterator) Describe() graph.Description {
	size, _ := n.Size()
	return graph.Description{
		UID:  n.UID(),
		Name: fmt.Sprintf("SQL_NODE_QUERY: %#v", n),
		Type: n.Type(),
		Size: size,
	}
}

func (n *SQLNodeIterator) Stats() graph.IteratorStats {
	size, _ := n.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}

func (n *SQLNodeIterator) Contains(v graph.Value) bool {
	// STUB
	return false
}

func (n *SQLNodeIterator) NextPath() bool {
	n.resultIndex += 1
	if n.resultIndex >= len(n.resultList) {
		return false
	}
	n.buildResult(n.resultIndex)
	return true
}

func (n *SQLNodeIterator) buildResult(i int) {
	container := n.resultList[i]
	n.result = make(map[string]string)
	for i, c := range n.cols {
		n.result[c] = container[i]
	}
}
