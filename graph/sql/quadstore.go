package sql

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

const QuadStoreType = "sql"

func init() {
	graph.RegisterQuadStore(QuadStoreType, true, newQuadStore, createSQLTables, nil)
}

type QuadStore struct {
	db *sql.DB
}

func connectSQLTables(addr string, _ graph.Options) (*sql.DB, error) {
	// TODO(barakmich): Parse options for more friendly addr.
	conn, err := sql.Open("postgres", addr)
	if err != nil {
		glog.Errorf("Couldn't open database at %s: %#v", addr, err)
		return nil, err
	}
	return conn, nil
}

func createSQLTables(addr string, options graph.Options) error {
	conn, err := connectSQLTables(addr, options)
	if err != nil {
		return err
	}
	tx, err := conn.Begin()
	if err != nil {
		glog.Errorf("Couldn't begin creation transaction: %s", err)
		return err
	}

	tripleTable, err := tx.Exec(`
	CREATE TABLE quads (
		subject TEXT NOT NULL,
		predicate TEXT NOT NULL,
		object TEXT NOT NULL,
		label TEXT,
		horizon BIGSERIAL PRIMARY KEY,
		UNIQUE(subject, predicate, object, label)
	);`)
	if err != nil {
		return err
	}
	nodeTable, err := tx.Exec(`
	CREATE TABLE nodes (
		node TEXT NOT NULL,
		size BIGINT NOT NULL
	);`)
	if err != nil {
		return err
	}
	index, err := tx.Exec(`
	CREATE INDEX spo_index ON quads (subject, predicate, object);
	CREATE INDEX pos_index ON quads (predicate, object, subject);
	CREATE INDEX osp_index ON quads (object, subject, predicate);
	CREATE INDEX cps_index ON quads (label, predicate, subject);
	`)
	if err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	conn, err := connectSQLTables(addr, options)
	if err != nil {
		return nil, err
	}
	qs.db = conn
	return &qs, nil
}

func (qs *QuadStore) getIDForQuad(t quad.Quad) string {
	return fmt.Sprintf("(%s, %s, %s, %s)")
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	return nil
}

func (qs *QuadStore) Quad(val graph.Value) quad.Quad {
	return val.(quad.Quad)
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewIterator(qs, d, val)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *QuadStore) ValueOf(s string) graph.Value {
	return s
}

func (qs *QuadStore) NameOf(v graph.Value) string {
	return v.(string)
}

func (qs *QuadStore) Size() int64 {
	var count int64
	c := qs.db.QueryRow("SELECT COUNT(*) FROM quads;")
	err := c.Scan(&count)
	if err != nil {
		glog.Errorf("Couldn't execute COUNT: %v", err)
		return 0
	}
	return count
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	var horizon int64
	err := qs.db.QueryRow("SELECT horizon FROM quads ORDER BY horizon DESC LIMIT 1;").Scan(&horizon)
	if err != nil {
		glog.Errorf("Couldn't execute horizon: %v", err)
		return graph.NewSequentialKey(0)
	}
	return graph.NewSequentialKey(horizon)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) Close() {
	qs.db.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	q := in.(quad.Quad)
	return q.Get(d)
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))

	}
	return it, false
}

func (qs *QuadStore) optimizeLinksTo(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	if primary.Type() == graph.Fixed {
		size, _ := primary.Size()
		if size == 1 {
			if !graph.Next(primary) {
				panic("unexpected size during optimize")
			}
			val := primary.Result()
			newIt := qs.QuadIterator(it.Direction(), val)
			nt := newIt.Tagger()
			nt.CopyFrom(it)
			for _, tag := range primary.Tagger().Tags() {
				nt.AddFixed(tag, val)
			}
			it.Close()
			return newIt, true
		}
	}
	return it, false
}
