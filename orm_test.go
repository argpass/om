package om

import "testing"

func TestGetColumns(t *testing.T)  {
	type Email struct {
		Value string `db:"email,option"`
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
	if cols[2] != "email" {
		t.Errorf("expect #2 is email, got:%s", cols[2])
	}

	var authors []Author
	tp, err := extractModelType(&authors)
	if err != nil {
		t.Errorf("err:%v", err)
	}
	t.Logf("tp:%+v", tp)
	cols = getColumns(tp)
	t.Logf("cols:%v", cols)
}

