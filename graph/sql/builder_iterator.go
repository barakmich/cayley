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
	"strings"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

var sqlBuilderType graph.Type

func init() {
	sqlBuilderType = graph.RegisterIterator("sqlbuilder")
}

type tableDir struct {
	table string
	dir   quad.Direction
}

func (td tableDir) String() string {
	if td.table != "" {
		return fmt.Sprintf("%s.%s", td.table, td.dir)
	}
	return "ERR"
}

type clause interface {
	toSQL() (string, []string)
	getTables() map[string]bool
}

type baseClause struct {
	pair      tableDir
	strTarget []string
	target    tableDir
}

func (b baseClause) toSQL() (string, []string) {
	if len(b.strTarget) > 1 {
		// TODO(barakmich): Sets of things, IN clause
		return "", []string{}
	}
	if len(b.strTarget) == 0 {
		return fmt.Sprintf("%s = %s", b.pair, b.target), []string{}
	}
	return fmt.Sprintf("%s = ?", b.pair), []string{b.strTarget[0]}
}

func (b baseClause) getTables() map[string]bool {
	out := make(map[string]bool)
	if b.pair.table != "" {
		out[b.pair.table] = true
	}
	if b.target.table != "" {
		out[b.target.table] = true
	}
	return out
}

type joinClause struct {
	left  clause
	right clause
	op    clauseOp
}

func (jc joinClause) toSQL() (string, []string) {
	l, lstr := jc.left.toSQL()
	r, rstr := jc.right.toSQL()
	lstr = append(lstr, rstr...)
	var op string
	switch jc.op {
	case andClause:
		op = "AND"
	case orClause:
		op = "OR"
	}
	return fmt.Sprint("(%s %s %s)", l, op, r), lstr
}

func (jc joinClause) getTables() map[string]bool {
	m := jc.left.getTables()
	for k, _ := range jc.right.getTables() {
		m[k] = true
	}
	return m
}

type tag struct {
	pair tableDir
	t    string
}

type statementType int

const (
	node statementType = iota
	link
)

type clauseOp int

const (
	andClause clauseOp = iota
	orClause
)

func (it *StatementIterator) canonicalizeWhere() (string, []string) {
	b := it.buildWhere
	b.pair.table = fmt.Sprintf("t_%d", it.uid)
	return b.toSQL()
}

func (it *StatementIterator) getTables() map[string]bool {
	m := make(map[string]bool)
	if it.where != nil {
		m = it.where.getTables()
	}
	for _, t := range it.tags {
		if t.pair.table != "" {
			m[t.pair.table] = true
		}
	}
	return m
}

func (it *StatementIterator) buildQuery() (string, []string) {
	str := "SELECT "
	var t []string
	if it.stType == link {
		t = []string{
			fmt.Sprintf("t_%d.subject", it.uid),
			fmt.Sprintf("t_%d.predicate", it.uid),
			fmt.Sprintf("t_%d.object", it.uid),
			fmt.Sprintf("t_%d.label", it.uid),
		}
	} else {
		t = []string{fmt.Sprintf("t_%d.%s as __execd", it.uid, it.dir)}
	}
	for _, v := range it.tags {
		t = append(t, fmt.Sprintf("%s as %s", v.pair, v.t))
	}
	str += strings.Join(t, ", ")
	str += " FROM "
	t = []string{fmt.Sprintf("quads as t_%d", it.uid)}
	for k, _ := range it.getTables() {
		t = append(t, fmt.Sprintf("quads as %s", k))
	}
	str += strings.Join(t, ", ")
	str += " WHERE "
	s, values := it.canonicalizeWhere()
	str += s
	if it.where != nil {
		s += " AND "
		where, v2 := it.where.toSQL()
		s += where
		values = append(values, v2...)
	}
	if it.stType == node {
		str += " ORDER BY __execd"
	}
	str += ";"
	for i := 1; i <= len(values); i++ {
		str = strings.Replace(str, "?", fmt.Sprintf("$%d", i), 1)
	}
	return str, values
}

type StatementIterator struct {
	uid        uint64
	qs         *QuadStore
	buildWhere baseClause
	where      clause
	tagger     graph.Tagger
	tags       []tag
	err        error
	cursor     *sql.Rows
	stType     statementType
	dir        quad.Direction
	result     map[string]string
	resultQuad quad.Quad
}

func (it *StatementIterator) Clone() graph.Iterator {
	m := &StatementIterator{
		uid:        iterator.NextUID(),
		qs:         it.qs,
		buildWhere: it.buildWhere,
		where:      it.where,
		stType:     it.stType,
	}
	copy(it.tags, m.tags)
	m.tagger.CopyFrom(it)
	return m
}

func NewStatementIterator(qs *QuadStore, d quad.Direction, val string) *StatementIterator {
	it := &StatementIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		buildWhere: baseClause{
			pair:      tableDir{"", d},
			strTarget: []string{val},
		},
		stType: link,
	}
	return it
}

func (it *StatementIterator) UID() uint64 {
	return it.uid
}

func (it *StatementIterator) Reset() {
	it.err = nil
	it.Close()
}

func (it *StatementIterator) Err() error {
	return it.err
}

func (it *StatementIterator) Close() error {
	if it.cursor != nil {
		err := it.cursor.Close()
		if err != nil {
			return err
		}
		it.cursor = nil
	}
	return nil
}

func (it *StatementIterator) Tagger() *graph.Tagger {
	return &it.tagger
}

func (it *StatementIterator) Result() graph.Value {
	if it.stType == node {
		return it.result["__execd"]
	}
	return it.resultQuad
}

func (it *StatementIterator) NextPath() bool {
	return false
}

func (it *StatementIterator) TagResults(dst map[string]graph.Value) {
	for tag, value := range it.result {
		dst[tag] = value
	}

	for tag, value := range it.tagger.Fixed() {
		dst[tag] = value
	}
}

func (it *StatementIterator) Type() graph.Type {
	return sqlBuilderType
}

func (it *StatementIterator) Contains(v graph.Value) bool {
	return false
}

func (it *StatementIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *StatementIterator) Sorted() bool                     { return false }
func (it *StatementIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *StatementIterator) Size() (int64, bool) { return 1, false }

func (it *StatementIterator) Describe() graph.Description {
	return graph.Description{
		UID:  it.UID(),
		Name: "SQL_QUERY",
		Type: it.Type(),
		Size: 1,
	}
}

func (it *StatementIterator) Stats() graph.IteratorStats {
	return graph.IteratorStats{
		ContainsCost: 10,
		NextCost:     5,
		Size:         1,
	}
}

func (it *StatementIterator) makeCursor() {
	if it.cursor != nil {
		it.cursor.Close()
	}
	q, values := it.buildQuery()
	ivalues := make([]interface{}, 0, len(values))
	for _, v := range values {
		ivalues = append(ivalues, v)
	}
	cursor, err := it.qs.db.Query(q, ivalues...)
	if err != nil {
		glog.Errorln("Couldn't get cursor from SQL database: %v", err)
		cursor = nil
	}
	it.cursor = cursor
}
func (it *StatementIterator) Next() bool {
	graph.NextLogIn(it)
	if it.cursor == nil {
		it.makeCursor()
	}
	if !it.cursor.Next() {
		glog.V(4).Infoln("sql: No next")
		err := it.cursor.Err()
		if err != nil {
			glog.Errorf("Cursor error in SQL: %v", err)
			it.err = err
		}
		it.cursor.Close()
		return false
	}
	cols, err := it.cursor.Columns()
	if err != nil {
		glog.Errorf("Couldn't get columns")
		it.err = err
		it.cursor.Close()
		return false
	}
	pointers := make([]interface{}, len(cols))
	container := make([]string, len(cols))
	for i, _ := range pointers {
		pointers[i] = &container[i]
	}
	err = it.cursor.Scan(pointers...)
	if err != nil {
		glog.Errorf("Error nexting iterator: %v", err)
		it.err = err
		return false
	}
	if it.stType == node {
		it.result = make(map[string]string)
		for i, c := range cols {
			it.result[c] = container[i]
		}
		return true
	}
	var q quad.Quad
	q.Subject = container[0]
	q.Predicate = container[1]
	q.Object = container[2]
	q.Label = container[3]
	it.resultQuad = q
	it.result = make(map[string]string)
	for i, c := range cols[4:] {
		it.result[c] = container[i+4]
	}
	return graph.NextLogOut(it, it.result, true)
}
