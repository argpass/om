package om

import (
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"strings"
	"reflect"
	"fmt"
)

const (
	tag = "db"
)

var (
	modelsMapper = reflectx.NewMapperFunc(tag, func(s string) string {
		return strings.ToLower(s)
	})
)

// getColumns returns mapping column names of the model `m`
func getColumns(tOrModel interface{}) (cols []string) {
	var tp reflect.Type
	var ok bool
	tp, ok = tOrModel.(reflect.Type)
	if !ok {
		v := reflect.Indirect(reflect.ValueOf(tOrModel))
		tp = v.Type()
	}
	tpMap := modelsMapper.TypeMap(tp)
	for name, info := range tpMap.Names {
		_, ok := info.Field.Tag.Lookup(tag)
		if ok {
			cols = append(cols, name)
		}
	}
	return cols
}

func extractModelType(dest interface{}) (tp reflect.Type, err error) {
	v := reflect.Indirect(reflect.ValueOf(dest))
	if v.Kind() != reflect.Slice {
		return tp, fmt.Errorf("need slice type, got %v", v.Kind())
	}
	el := v.Type().Elem()
	return el, nil
}

type Query struct {
}

func (q *Query) Where() {
}

type Model struct {
}

type isModel interface {
	isModel() bool
}

type M struct {
}

func (m *M) isModel() bool {
	return true
}

type Iterator interface {
	Next() bool
	Get(dest interface{}) bool
	Close() error
}

type Donner interface {
	Done() (int64,error)
}

var _ Donner = &executor{}
type executor struct {
	isInsert bool
	err error
	tb *Tables
	sql string
	args []interface{}
}

func (e *executor) Done() (int64, error) {
	if e.err != nil {
		return 0, e.err
	}
	r, err := e.tb.db.dbx.Exec(e.sql, e.args...)
	if err != nil {
		e.err = err
		return 0, e.err
	}
	if e.isInsert {
		return r.LastInsertId()
	}
	return r.RowsAffected()
}

type IsQueryResult interface {
	Get(m isModel) error
	All(models interface{}) error
	Iter(it *Iterator) error
	SliceMap(*[]map[string]interface{}) error
}

type IsResult interface {
	Donner
	IsQueryResult
}

type queryResult struct {
	query string
	args []interface{}
	db *DB
	err error
}

func(qr *queryResult) All(models interface{}) error {
	if qr.err != nil {
		return qr.err
	}
	qr.err = qr.db.dbx.Select(models, qr.query, qr.args...)
	return qr.err
}
func(qr *queryResult) Iter(it *Iterator) error {return nil}
func(qr *queryResult) SliceMap(dest *[]map[string]interface{}) error {
	if qr.err != nil {
		return qr.err
	}
	rows, err := qr.db.dbx.Queryx(qr.query, qr.args...)
	qr.err = err
	if qr.err != nil {
		return qr.err
	}
	var data []map[string]interface{}
	for rows.Next() {
		cur := map[string]interface{}{}
		qr.err = rows.MapScan(cur)
		if qr.err != nil {
			return qr.err
		}
		data = append(data, cur)
	}
	*dest = data
	return qr.err
}
func (qr *queryResult)Get(m isModel) error  {
	// has err ? return right now
	if qr.err != nil {
		return qr.err
	}
	qr.err = qr.db.dbx.Get(m, qr.query, qr.args...)
	return qr.err
}


type DB struct {
	dbx *sqlx.DB
}

func NewDB(db *sqlx.DB) *DB {
	return &DB{db}
}

func (d *DB) Query(sql string, args...interface{}) IsQueryResult {
	return &queryResult{
		db:d,
		query:sql,
		args:args,
	}
}

func (d *DB) Exec(sql string, args...interface{}) Donner {
	return nil
}

func (m *DB) Begin() *DB {
	return nil
}

func (m *DB) Commit() error {
	return nil
}

func (m *DB) Tb(table string, alias ...string) *Tables {
	return NewTables(m, table, alias...)
}
