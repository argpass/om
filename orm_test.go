package om

import (
	"testing"
	"reflect"
)

func TestGetColumns(t *testing.T)  {
	type Email struct {
		Value string `db:"goto,option"`
	}
	type Author struct {
		M
		Age int `db:"age, required"`
		Name string  `db:"name,required"`
		Email

		id int
	}
	author := Author{Age:99}
	cols := getColumns(author)
	if len(cols) != 3 {
		t.Errorf("expect 3 columns got:%d", len(cols))
	}

	var authors []Author
	tp, err := extractModelType(&authors)
	if err != nil {
		t.Errorf("err:%v", err)
	}
	t.Logf("tp:%+v", tp)
	cols = getColumns(tp)
	t.Logf("cols:%v", cols)

	v := modelsMapper.FieldByName(reflect.ValueOf(&author), "goto")
	t.Logf("v:%+v", v)
}

func TestParseSliceIn(t *testing.T) {
	q := "insert into t (a,b) values(?,?)"
	//args := []interface{}{[]string{"abc", "efg"}}
	args := []interface{}{"abc", "efg"}

	err := parseINSpec(&q, &args)
	if err != nil {
		t.Errorf("err:%v", err)
	}
	t.Logf("q:%s, args:%v", q, args)
}

