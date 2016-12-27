package om

import (
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"strings"
	"reflect"
	"fmt"
	"database/sql"
	"github.com/Sirupsen/logrus"
)

const (
	tag = "db"
)

var (
	modelsMapper = reflectx.NewMapperFunc(tag, func(s string) string {
		return strings.ToLower(s)
	})
)

type Manager struct {
	v          reflect.Value
	tp         reflect.Type
	model      isModel
	colInfoMap map[string]*reflectx.FieldInfo
	fieldMap map[string]reflect.Value
	tpMap      *reflectx.StructMap
}

func newManager(model isModel) (*Manager, error)  {
	v := reflect.Indirect(reflect.ValueOf(model))
	tp := v.Type()
	tpMap := modelsMapper.TypeMap(tp)
	var colsMap = map[string] *reflectx.FieldInfo {}
	for name, info := range tpMap.Names {
		_, ok := info.Field.Tag.Lookup(tag)
		if ok {
			colsMap[name] = info
		}
	}
	m := &Manager{
		model:model,
		colInfoMap:colsMap,
		tpMap:tpMap,
		fieldMap:modelsMapper.FieldMap(v),
		v:v,
		tp:tp,
	}
	return m, nil
}

func (m *Manager) ColsMap() map[string]interface{} {
	var colsMap = map[string] interface{}{}
	for col, _ := range m.colInfoMap {
		colsMap[col] = m.fieldMap[col].Interface()
	}
	return colsMap
}

// Bind try to set model status as bind
func (m *Manager)Bind(id int64) {
	// todo: bind id
}

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

type idHolder interface {
	Identity() (colName string, value interface{})
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
	callback func() (int64, error)
}

func (e *executor) Done() (int64, error)  {
	return e.callback()
}

//func (e *executor) Done() (int64, error) {
//	if e.err != nil {
//		return 0, e.err
//	}
//	r, err := e.tb.db.dbx.Exec(e.sql, e.args...)
//	if err != nil {
//		e.err = err
//		return 0, e.err
//	}
//	if e.isInsert {
//		return r.LastInsertId()
//	}
//	return r.RowsAffected()
//}

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

type SQLLogger interface {
	Debug(spec string, query string, args []interface{})
	Error(spec string, err error, query string, args []interface{})
}

type wrappedDB struct {
	DB     *sqlx.DB
	logger SQLLogger
}

//func (w *wrappedDB) Query(query string, args...interface{}) (*sql.Rows, error) {
//	err := parseSliceArg(&query, &args)
//	if err != nil {
//		return nil, err
//	}
//	fmt.Printf("[SQL-Query]%s, --args:%+v\n",query, args)
//	return w.DB.Query(query, args...)
//}

func (w *wrappedDB) Queryx(query string, args...interface{}) (rows *sqlx.Rows, err error) {
	err = parseINSpec(&query, &args)
	if err != nil {
		return nil, err
	}
	w.logger.Debug("[Queryx]", query, args)
	rows, err = w.DB.Queryx(query, args...)
	if err != nil {
		w.logger.Error("[Queryx]", err, query, args)
	}
	return rows, err
}

func (w *wrappedDB) Get(dest interface{}, query string, args ...interface{}) (err error) {
	err = parseINSpec(&query, &args)
	if err != nil {
		return err
	}
	w.logger.Debug("[Get]", query, args)
	err = w.DB.Get(dest, query, args...)
	if err != nil {
		w.logger.Error("[Get]", err, query, args)
	}
	return err
}

func (w *wrappedDB) Select(dest interface{}, query string, args ...interface{}) (err error) {
	err = parseINSpec(&query, &args)
	if err != nil {
		return err
	}
	w.logger.Debug("[Select]", query, args)
	err =  w.DB.Select(dest, query, args...)
	if err != nil {
		w.logger.Error("[Select]", err, query, args)
	}
	return err
}

func (w *wrappedDB) Exec(query string, args...interface{}) (re sql.Result, err error) {
	err = parseINSpec(&query, &args)
	if err != nil {
		return nil, err
	}
	w.logger.Debug("[Exec]", query, args)
	re, err = w.DB.Exec(query, args...)
	if err != nil {
		w.logger.Error("[Exec]", err, query, args)
	}
	return re, err
}

type DB struct {
	dbx *wrappedDB
}

type sqlLogger struct {
	logger *logrus.Entry
}

func (log *sqlLogger) Debug(spec string, query string, args []interface{})  {
}

func (log *sqlLogger) Error(spec string, query string, args []interface{})  {
}

// todo: resove SQLLogger inner
func NewDB(db *sqlx.DB, logger *logrus.Entry) *DB {
	w := &wrappedDB{DB:db, logger:logger}
	return &DB{w}
}

func (m *DB) Tb(table string, alias ...string) *Tables {
	return NewTables(m, table, alias...)
}
