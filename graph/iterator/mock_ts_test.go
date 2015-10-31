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

package iterator

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

// oldstore is a mocked version of the QuadStore interface, for use in tests.
type oldstore struct {
	data []string
	iter graph.FixedIterator
}

func (qs *oldstore) ValueOf(s string) graph.Value {
	for i, v := range qs.data {
		if s == v {
			return i
		}
	}
	return nil
}

func (qs *oldstore) ApplyDeltas([]graph.Delta, graph.IgnoreOpts) error { return nil }

func (qs *oldstore) Quad(graph.Value) quad.Quad { return quad.Quad{} }

func (qs *oldstore) QuadIterator(d quad.Direction, i graph.Value) graph.Iterator {
	return qs.iter
}

func (qs *oldstore) NodesAllIterator() graph.Iterator { return &Null{} }

func (qs *oldstore) QuadsAllIterator() graph.Iterator { return &Null{} }

func (qs *oldstore) NameOf(v graph.Value) string {
	switch v.(type) {
	case int:
		i := v.(int)
		if i < 0 || i >= len(qs.data) {
			return ""
		}
		return qs.data[i]
	case string:
		return v.(string)
	default:
		return ""
	}
}

func (qs *oldstore) Size() int64 { return 0 }

func (qs *oldstore) Horizon() graph.PrimaryKey { return graph.NewSequentialKey(0) }

func (qs *oldstore) DebugPrint() {}

func (qs *oldstore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return &Null{}, false
}

func (qs *oldstore) FixedIterator() graph.FixedIterator {
	return NewFixed(Identity)
}

func (qs *oldstore) Close() {}

func (qs *oldstore) QuadDirection(graph.Value, quad.Direction) graph.Value { return 0 }

func (qs *oldstore) RemoveQuad(t quad.Quad) {}

func (qs *oldstore) Type() string { return "oldmockstore" }

type store struct {
	data []quad.Quad
}

var _ graph.QuadStore = &store{}

func (qs *store) ValueOf(s string) graph.Value {
	return s
}

func (qs *store) ApplyDeltas([]graph.Delta, graph.IgnoreOpts) error { return nil }

func (qs *store) Quad(v graph.Value) quad.Quad { return v.(quad.Quad) }

func (qs *store) NameOf(v graph.Value) string {
	return v.(string)
}

func (qs *store) RemoveQuad(t quad.Quad) {}

func (qs *store) Type() string { return "mockstore" }

func (qs *store) QuadDirection(v graph.Value, d quad.Direction) graph.Value {
	return qs.Quad(v).Get(d)
}

func (qs *store) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return &Null{}, false
}

func (qs *store) FixedIterator() graph.FixedIterator {
	return NewFixed(Identity)
}

func (qs *store) Close() {}

func (qs *store) Horizon() graph.PrimaryKey { return graph.NewSequentialKey(0) }

func (qs *store) DebugPrint() {}

func (qs *store) QuadIterator(d quad.Direction, i graph.Value) graph.Iterator {
	fixed := qs.FixedIterator()
	for _, q := range qs.data {
		if q.Get(d) == i.(string) {
			fixed.Add(q)
		}
	}
	return fixed
}

func (qs *store) NodesAllIterator() graph.Iterator {
	set := make(map[string]bool)
	for _, q := range qs.data {
		for _, d := range quad.Directions {
			set[q.Get(d)] = true
		}
	}
	fixed := qs.FixedIterator()
	for k, _ := range set {
		fixed.Add(k)
	}
	return fixed
}

func (qs *store) QuadsAllIterator() graph.Iterator {
	fixed := qs.FixedIterator()
	for _, q := range qs.data {
		fixed.Add(q)
	}
	return fixed
}

func (qs *store) Size() int64 { return int64(len(qs.data)) }
