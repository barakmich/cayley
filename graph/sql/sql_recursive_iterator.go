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
	"errors"
	"fmt"
	"strings"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

type SQLRecursive struct {
	recTableName string

	nodeIt    sqlIterator
	nodetable string
	stepIt    sqlIterator
	size      int64
	tagger    graph.Tagger

	result string
}

func newRecursive(nodeIt sqlIterator, morphism graph.ApplyMorphism) (*SQLRecursive, error) {
	all := NewAllIterator(nil, "nodes")
	all.Tagger().Add("__rec_base")
	it := morphism(nil, all)
	newIt, ok := it.Optimize()
	if !ok {
		return nil, errors.New("couldn't optimize")
	}
	if newIt.Type() != sqlType {
		return nil, errors.New("couldn't collapse into subiterator")
	}
	sqlit := newIt.(*SQLIterator)
	return &SQLRecursive{
		recTableName: newNodeTableName(),
		nodeIt:       nodeIt,
		nodetable:    newNodeTableName(),
		stepIt:       sqlit.sql,
	}, nil

}

func (r *SQLRecursive) sqlClone() sqlIterator {
	m := &SQLRecursive{
		recTableName: newNodeTableName(),
		nodeIt:       r.nodeIt.sqlClone(),
		nodetable:    newNodeTableName(),
		stepIt:       r.stepIt.sqlClone(),
		size:         r.size,
	}
	m.tagger.CopyFromTagger(r.Tagger())
	return m
}

func (r *SQLRecursive) Tagger() *graph.Tagger {
	return &r.tagger
}

func (r *SQLRecursive) Result() graph.Value {
	return r.result
}

func (r *SQLRecursive) Type() sqlQueryType {
	return recursive
}

func (r *SQLRecursive) Size(qs *QuadStore) (int64, bool) {
	return qs.Size(), true
}

func (r *SQLRecursive) Describe() string {
	s, _ := r.buildSQL(true, nil)
	return fmt.Sprintf("SQL_RECURSIVE: %s", s)
}

func (r *SQLRecursive) buildResult(result []string, cols []string) map[string]string {
	m := make(map[string]string)
	for i, c := range cols {
		if strings.HasSuffix(c, "_hash") {
			continue
		}
		if strings.HasPrefix(c, "__rec") {
			continue
		}
		if c == "__execd" {
			r.result = result[i]
		}
		m[c] = result[i]
	}
	return m
}

func (r *SQLRecursive) getTables() []tableDef {
	var td tableDef
	var table string
	table, td.values = r.buildRecursiveSubquery()
	td.table = fmt.Sprintf("\n(%s)", table)
	td.name = r.recTableName
	return []tableDef{td}
}

var frontParams = []string{"__rec_id", "__rec_depth", "__rec_path", "__rec_cycle"}

func (r *SQLRecursive) buildRecursiveSubquery() (string, []string) {
	var values []string
	query := "WITH RECURSIVE\n"
	query += r.recTableName + "("
	tags := r.nodeIt.getTags()
	params := append([]string{}, frontParams...)
	for _, v := range tags {
		params = append(params, v.tag)
	}
	query += strings.Join(params, ",")
	query += ") AS (\n"
	base, baseval := r.buildBaseCase()
	query += base
	values = append(values, baseval...)
	query += "\nUNION\n"
	rec, recval := r.buildRecursiveStep()
	query += rec
	values = append(values, recval...)
	query += "\n)\n"
	query += "SELECT "
	query += strings.Join(params[4:], ",")
	query += " FROM " + r.recTableName + " "
	return query, values
}

func (r *SQLRecursive) buildBaseCase() (string, []string) {
	topData := r.nodeIt.tableID()
	tags := []tagDir{topData}
	tags = append(tags, r.nodeIt.getTags()...)
	query := "SELECT "
	t := []string{"__rec_id", "0", "ARRAY[__rec_id]", "false"}
	for _, v := range tags {
		t = append(t, v.String())
	}
	query += strings.Join(t, ", ")
	query += " FROM "

	t = []string{}
	var values []string
	for _, k := range r.nodeIt.getTables() {
		values = append(values, k.values...)
		t = append(t, fmt.Sprintf("%s as %s", k.table, k.name))
	}

	query += strings.Join(t, ", ")
	query += " WHERE "

	constraint, wherevalues := r.nodeIt.buildWhere()
	values = append(values, wherevalues...)
	if constraint != "" {
		constraint += " AND "
	}
	constraint += fmt.Sprintf("__rec_id = %s", topData.TableHashOnly())

	query += constraint
	return query, values
}

func (r *SQLRecursive) buildRecursiveStep() (string, []string) {
	topData := r.stepIt.tableID()
	tags := []tagDir{topData}
	tags = append(tags, r.nodeIt.getTags()...)
	query := "SELECT "
	t := []string{"__rec_id", "prev.__rec_depth + 1", "__rec_path || __rec_id", "__rec_id = ANY(__rec_path)"}
	for _, v := range tags {
		t = append(t, fmt.Sprintf("prev.%s", v.tag))
	}
	query += strings.Join(t, ", ")
	query += " FROM "

	t = []string{}
	var values []string
	for _, k := range r.nodeIt.getTables() {
		values = append(values, k.values...)
		t = append(t, fmt.Sprintf("%s as %s", k.table, k.name))
	}
	t = append(t, fmt.Sprintf("%s as prev", r.recTableName))

	query += strings.Join(t, ", ")
	query += " WHERE "

	constraint, wherevalues := r.nodeIt.buildWhere()
	values = append(values, wherevalues...)
	if constraint != "" {
		constraint += " AND "
	}
	steptags := r.stepIt.getTags()
	var found tagDir
	for _, s := range steptags {
		if s.tag == "__rec_base" {
			found = s
			break
		}
	}
	constraint += fmt.Sprintf("__rec_id = %s", topData.TableHashOnly())
	constraint += fmt.Sprintf("AND %s = prev.__rec_id", found.TableHashOnly())
	constraint += "AND NOT __rec_cycle"

	query += constraint
	return query, values
}

func (r *SQLRecursive) tableID() tagDir {
	return tagDir{
		table: r.recTableName,
		dir:   quad.Any,
		tag:   "__execd",
	}
}

func (r *SQLRecursive) getTags() []tagDir {
	return r.nodeIt.getTags()
}

func (r *SQLRecursive) buildWhere() (string, []string) {
	return "", []string{}
}

func (r *SQLRecursive) sameTopResult(target []string, test []string) bool {
	return target[0] == test[0]
}

func (r *SQLRecursive) buildSQL(next bool, val graph.Value) (string, []string) {
	topData := r.tableID()
	tags := []tagDir{topData}
	tags = append(tags, r.getTags()...)
	query := "SELECT "
	var t []string
	for _, v := range tags {
		t = append(t, v.String())
	}
	query += strings.Join(t, ", ")
	query += " FROM "
	t = []string{}
	var values []string
	for _, k := range r.getTables() {
		values = append(values, k.values...)
		t = append(t, fmt.Sprintf("%s as %s", k.table, k.name))
	}
	query += strings.Join(t, ", ")
	query += " WHERE "

	constraint, wherevalues := r.buildWhere()
	values = append(values, wherevalues...)

	if !next {
		v := val.(string)
		if constraint != "" {
			constraint += " AND "
		}
		constraint += fmt.Sprintf("%s.%s_hash = ?", topData.table, topData.dir)
		values = append(values, hashOf(v))
	}
	query += constraint
	query += ";"

	if glog.V(4) {
		dstr := query
		for i := 1; i <= len(values); i++ {
			dstr = strings.Replace(dstr, "?", fmt.Sprintf("'%s'", values[i-1]), 1)
		}
		glog.V(4).Infoln(dstr)
	}
	return query, values
}

func (r *SQLRecursive) quickContains(_ graph.Value) (bool, bool) { return false, false }
