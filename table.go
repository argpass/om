package om

import (
	"strings"
	"errors"
	"fmt"
	"bytes"
	"github.com/jmoiron/sqlx"
	"reflect"
)

type DeferWhere struct {
	tb *Tables
	colsMap map[string]interface{}
	cb func(w *DeferWhere) (int64, error)

	where string
	args []interface{}
}

// Where method attaches where condition and args
func (w *DeferWhere) Where(where string, args...interface{}) Donner {
	w.where = where
	w.args = args
	exec := &executor{
		callback:func() (int64, error){
			if w.tb.err != nil {
				return 0, w.tb.err
			}
			return w.cb(w)
		},
	}
	return exec
}

// Done ends up deferring process right now
func (w *DeferWhere) Done() (int64, error) {
	if w.tb.err != nil {
		return 0, w.tb.err
	}
	return w.cb(w)
}

type Select struct {
	err error
	tb *Tables
	cols []string

	where string
	args []interface{}

	orderCols []string
	// default is asc
	orderDesc bool

	// [0]:begin, [1]end, nil: no limit
	limit []int
}

func parseINSpec(pquery *string, pargs *[]interface{}) error {
	query := *pquery
	// check if has `IN` spec
	if !strings.Contains(strings.ToUpper(query), "IN") {
		return nil
	}
	queryS := strings.Split(query, "?")
	sb := bytes.NewBufferString("")
	args := *pargs
	var newArgs []interface{}
	var j int
	var arg interface{}
	for j, arg = range args {
		sb.WriteString(queryS[j])
		v := reflect.ValueOf(arg)
		if v.Kind() == reflect.Slice {
			sArgs := make([]interface{}, v.Len())
			for i:= 0; i < v.Len(); i++ {
				sArgs[i] = v.Index(i).Interface()
			}
			sb.WriteString("(")
			bts := bytes.Repeat([]byte{'?', ','}, len(sArgs))
			bts[len(bts) - 1] = ')'
			sb.Write(bts)
			// extend newArgs
			for _, a := range sArgs {
				newArgs = append(newArgs, a)
			}
		}else{
			newArgs = append(newArgs, arg)
			sb.WriteString("?")
		}
	}
	if len(args) > 0 {
		sb.WriteString(queryS[j + 1])
	}
	*pquery = sb.String()
	*pargs = newArgs
	return nil
}

func NewSelect(tb *Tables, cols...string) *Select {
	return &Select{tb:tb,cols:cols, err:tb.err}
}

func(s *Select) Where(where string, args...interface{}) *Select {
	if s.where != "" {
		s.err = errors.New("where alreay set")
		return s
	}
	s.where = strings.Trim(where, " ")
	s.args = args
	return s
}

func (s *Select) OrderAsc(cols...string) *Select {
	s.orderCols = cols
	s.orderDesc = false
	return s
}

func (s *Select) OrderDesc(cols...string) *Select {
	s.orderDesc = true
	s.orderCols = cols
	return s
}

func (s *Select) Limit(begin int, end int) *Select {
	s.limit = []int{begin, end}
	return s
}

func (s *Select) toSql() (string, error) {
	var blocks []string
	if s.cols == nil {
		return "", errors.New("need selected column names")
	}
	cols := strings.Join(s.cols, ",")
	from, err := s.tb.toSql()
	if err != nil {
		return "", err
	}
	// select .. from ...join..
	_select := strings.Join([]string{"SELECT", cols, from}, " ")
	blocks = append(blocks, _select)
	// where...
	if s.where != "" {
		where := fmt.Sprintf("Where %s", s.where)
		blocks = append(blocks, where)
	}
	// order by ...
	if s.orderCols != nil {
		asc := "ASC"
		if s.orderDesc {
			asc = "DESC"
		}
		order := strings.Join([]string{"ORDER BY", strings.Join(s.orderCols, ","), asc}, " ")
		blocks = append(blocks, order)
	}
	// limit ..
	if s.limit != nil {
		limit := fmt.Sprintf("LIMIT %d, %d", s.limit[0], s.limit[1])
		blocks = append(blocks, limit)
	}
	return strings.Join(blocks, " "), nil
}

func (s *Select) Get(m isModel) error {
	if s.err != nil {
		return s.err
	}
	// get cols from the isModel
	if s.cols == nil {
		cols := getColumns(m)
		if cols == nil {
			s.err = errors.New("get none columns mapping on the model")
			return s.err
		}
		s.cols = cols
	}
	var q string
	q, s.err = s.toSql()
	if s.err != nil {
		return s.err
	}
	s.err = s.tb.db.dbx.Get(m, q, s.args...)
	return s.err
}

func (s *Select) All(models interface{}) error {
	if s.err != nil {
		return s.err
	}
	// get cols from the isModel
	if s.cols == nil {
		tp, err := extractModelType(models)
		if err != nil {
			s.err = err
			return s.err
		}
		cols := getColumns(tp)
		if cols == nil {
			s.err = errors.New("get none columns mapping on the model")
			return s.err
		}
		s.cols = cols
	}
	var q string
	q, s.err = s.toSql()
	if s.err != nil {
		return s.err
	}
	s.err = s.tb.db.dbx.Select(models, q, s.args...)
	return s.err
}

func (s *Select) MapScan(dest map[string]interface{}) error {
	if s.err != nil {
		return s.err
	}
	var q string
	q, s.err = s.toSql()
	if s.err != nil {
		return s.err
	}
	var rows *sqlx.Rows
	rows, s.err = s.tb.db.dbx.Queryx(q, s.args...)
	if s.err != nil {
		return s.err
	}
	for rows.Next() {
		s.err = rows.MapScan(dest)
		if s.err != nil {
			return s.err
		}
	}
	return s.err
}

//func (s *Select) Iter(it Iterator) error {
//	return nil
//}

type joinInfo struct {
	// join type
	tp string
	// join table
	tb string
	preAlias string
	// table alias
	alias string
	// on
	onLeft string
	onRight string
}

func (info *joinInfo) toSql() (string, error) {
	//leftGroup := strings.Split(info.onLeft, ".")
	onLeft := info.onLeft
	//if len(leftGroup) == 2 && leftGroup[0] != info.preAlias {
	//	return fmt.Errorf("invalid join on:%s", info.onLeft)
	//}
	//rightGroup := strings.Split(info.onRight, ".")
	onRight := info.onRight
	//if len(rightGroup) == 2 && rightGroup[0] != info.alias {
	//	return fmt.Errorf("invalid join on:%s", info.onRight)
	//}
	//if len(leftGroup) == 1 {
	//	onLeft = fmt.Sprintf("%s.%s", info.preAlias, onLeft)
	//}
	//if len(rightGroup) == 1 {
	//	onRight = fmt.Sprintf("%s.%s", info.alias, onRight)
	//}
	if strings.Contains(info.onLeft,
		strings.Join([]string{info.preAlias, "."}, "")) {
	}
	return strings.Join([]string{info.tp, info.tb, info.alias,
		"on", onLeft, "=", onRight}, " "), nil
}

type Tables struct {
	err error
	alias string
	db *DB
	name string
	joinInfos []*joinInfo
}

func (t *Tables) toSql() (string, error) {
	var err error
	js := make([]string, len(t.joinInfos))
	for i, join := range t.joinInfos {
		js[i], err = join.toSql()
		if err != nil {
			return "", err
		}
	}
	return strings.Join([]string{"FROM", t.name, t.alias,
		strings.Join(js, " ")}, " "), nil
}

func NewTables(db *DB, name string, alias...string) *Tables {
	t := &Tables{
		db:db,
		name:name,
		alias:name,
	}
	if len(alias) > 0 {
		t.alias = alias[0]
	}
	return t
}

func (t *Tables)Join(other string, onMyCol string, onOtherCol string, alias...string) *Tables {
	tp := "INNER JOIN"
	info := &joinInfo{
		tp:tp,
		tb:other,
		onLeft:onMyCol,
		onRight:onOtherCol,
		alias:other,
		preAlias:t.alias,
	}
	if len(alias) > 0 {
		info.alias = alias[0]
	}
	t.joinInfos = append(t.joinInfos, info)
	return t
}

func (t *Tables)LJ(other string, onMyCol string, onOtherCol string, alias...string) *Tables {
	tp := "LEFT JOIN"
	info := &joinInfo{
		tp:tp,
		tb:other,
		onLeft:onMyCol,
		onRight:onOtherCol,
		alias:other,
		preAlias:t.alias,
	}
	if len(alias) > 0 {
		info.alias = alias[0]
	}
	t.joinInfos = append(t.joinInfos, info)
	return t
}

func (t *Tables)RJ(other string, onMyCol string, onOtherCol string, alias...string) *Tables {
	tp := "RIGHT JOIN"
	info := &joinInfo{
		tp:tp,
		tb:other,
		onLeft:onMyCol,
		onRight:onOtherCol,
		alias:other,
		preAlias:t.alias,
	}
	if len(alias) > 0 {
		info.alias = alias[0]
	}
	t.joinInfos = append(t.joinInfos, info)
	return t
}

func (t *Tables)Select(cols...string) *Select {
	s := NewSelect(t, cols...)
	return s
}

func (t *Tables) Delete(ms ...isModel) *DeferWhere {
	var m isModel
	if len(ms) > 0 {
		m = ms[0]
	}
	w := &DeferWhere{
		tb:t,
		where:"",
		args:nil,
		cb:func(w *DeferWhere) (int64, error) {
			// no where condition, no id
			if w.where == "" {
				holder, ok := m.(idHolder)
				if !ok {
					w.tb.err = errors.New("no where condition and no id")
				}else{
					colName, id := holder.Identity()
					// build where string with id
					w.args = append(w.args, id)
					w.where = fmt.Sprintf("%s=?", colName)
				}
			}
			if w.tb.err != nil {
				return 0, w.tb.err
			}
			return w.tb.delete(w.where, w.args...)
		},
	}
	return w
}

func (t *Tables) delete(where string, args...interface{}) (int64, error)  {
	if t.err != nil {
		return 0, t.err
	}
	var whereSql = ""
	if where != "" {
		whereSql = fmt.Sprintf("WHERE %s", where)
	}
	query := fmt.Sprintf("DELETE FROM %s %s", t.name, whereSql)
	result, err := t.db.dbx.Exec(query, args...)
	if err!= nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (t *Tables) Update(m isModel) *DeferWhere {
	manager, err := newManager(m)
	if err != nil {
		t.err = err
	}
	w := &DeferWhere{
		tb:t,
		colsMap:manager.ColsMap(),
		where:"",
		args:nil,
		cb:func(w *DeferWhere)(int64, error) {
			// no where condition, no id
			if w.where == "" {
				holder, ok := m.(idHolder)
				if !ok {
					w.tb.err = errors.New("no where condition and no id")
				}else{
					colName, id := holder.Identity()
					// build where string with id
					w.args = append(w.args, id)
					w.where = fmt.Sprintf("%s=?", colName)
				}
			}
			if w.tb.err != nil {
				return 0, w.tb.err
			}
			cnt, err := t.update(w.colsMap, w.where, w.args...)
			return cnt, err
		},
	}
	return w
}

func (t *Tables) UpdateMap(colsMap map[string]interface{}) *DeferWhere {
	w := &DeferWhere{
		tb:t,
		colsMap:colsMap,
		where:"",
		args:nil,
		cb:func(w *DeferWhere) (int64, error) {
			if w.tb.err != nil {
				return 0, w.tb.err
			}
			return w.tb.update(w.colsMap, w.where, w.args...)
		},
	}
	return w
}

func (t *Tables) InsertMap(colsMap map[string]interface{}) Donner {
	e := &executor{
		callback:func() (int64, error){
			return t.insert(colsMap)
		},
	}
	return e
}

func (t *Tables) Insert(m isModel) Donner {
	manager, err := newManager(m)
	if err != nil {
		t.err = err
	}
	e := &executor{
		callback:func()(int64, error) {
			if t.err != nil {
				return 0, t.err
			}
			id, err := t.insert(manager.ColsMap())
			if err != nil {
				t.err = err
				return id, t.err
			}
			manager.Bind(id)
			return id, t.err
		},
	}
	return e
}

func (t *Tables) insert(colsMaps ...map[string]interface{}) (int64, error) {
	if t.err != nil {
		return 0, t.err
	}

	if len(t.joinInfos) > 0 {
		return 0, errors.New("un supportted join insert")
	}
	if len(colsMaps) == 0 {
		return 0, errors.New("no data to be insert")
	}
	var colNames []string
	var args []interface{}
	var values []string
	for i, aMap := range colsMaps {
		for name, arg := range aMap {
			if i == 0 {
				colNames = append(colNames, name)
			}
			args = append(args, arg)
		}
		bs := bytes.Repeat([]byte{'?',','}, len(aMap))
		bs[len(bs) - 1] = ')'
		values = append(values, strings.Join([]string{"(", string(bs)},""))
	}
	// Values: VALUES (?,?),(?,?),...
	valuesSql := fmt.Sprintf("VALUES %s", strings.Join(values, ","))
	if len(colNames) == 0 {
		return 0, errors.New("no columns")
	}
	// columns: colA, colB, colC,...
	names := strings.Join(colNames, ",")
	sql := fmt.Sprintf("INSERT INTO %s(%s) %s", t.name, names, valuesSql)
	result, err := t.db.dbx.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (t *Tables) update(colsMap map[string]interface{},
	where string, whereArgs...interface{}) (int64, error) {
	if t.err != nil {
		return 0, t.err
	}

	if len(t.joinInfos) > 0 {
		return 0, errors.New("un supportted join update")
	}
	if len(colsMap) == 0 {
		return 0, errors.New("no data to execute")
	}
	var cols []string
	var args []interface{}
	for name, arg := range colsMap {
		cols = append(cols, fmt.Sprintf("%s=?", name))
		args = append(args, arg)
	}
	whereSql := ""
	if where != "" {
		whereSql = fmt.Sprintf("WHERE %s", where)
		for _, arg := range whereArgs {
			args = append(args, arg)
		}
	}
	sql := fmt.Sprintf("UPDATE %s SET (%s) %s", t.name, cols, whereSql)
	result, err := t.db.dbx.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
