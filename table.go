package om

import (
	"strings"
	"errors"
	"fmt"
	"bytes"
)

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
		blocks = append(blocks, s.where)
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

func (s *Select) Iter(it Iterator) error {
	return nil
}

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

func (t *Tables) Insert(colsMap map[string]interface{}) Donner {
	return t.insert(colsMap)
}

func (t *Tables) insert(colsMaps ...map[string]interface{}) Donner {
	ec := &executor{
		isInsert:true,
		tb:t,
		err:t.err,
	}
	if t.err != nil {
		ec.err = t.err
		return ec
	}

	if len(t.joinInfos) > 0 {
		ec.err = errors.New("un supportted join insert")
		return ec
	}
	if len(colsMaps) == 0 {
		ec.err = errors.New("no data to be insert")
		return ec
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
		ec.err = errors.New("no columns")
		return ec
	}
	// columns: colA, colB, colC,...
	names := strings.Join(colNames, ",")
	ec.sql = fmt.Sprintf("INSERT INTO %s(%s) %s", t.name, names, valuesSql)
	ec.args = args
	return ec
}
