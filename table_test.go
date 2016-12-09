package om

import (
	"testing"
	"os"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
)

func init()  {
	// todo: remove this constant env key here
	os.Setenv("DBUTILS_MYSQL_DNS", "root:akun@123@(vagrant:3306)/test?charset=utf8")

	ConnectAll()
}

// clean testing tx
func clean (tx *sqlx.Tx){
	re := recover()
	var e error
	var ok bool
	if e, ok = re.(error); ok {
		fmt.Println("panic:", e)
		tx.Rollback()
	}else{
		tx.Commit()
	}
	if e != nil {
		panic(e)
	}
}

var TestMysql = true
var mysqlDB *sqlx.DB

type Scheme struct {
	create string
	drop string
}

func (p Scheme) Mysql() (string, string) {
	return p.create, p.drop
}

var t_book = "dbutils_t_book"

var test_scheme = Scheme{
	create:
	`CREATE TABLE dbutils_t_book(id INTEGER AUTO_INCREMENT PRIMARY KEY,name VARCHAR(100) NOT NULL,
	tag TINYINT NULL, deleted BOOLEAN DEFAULT FALSE)`,
	drop:"drop table dbutils_t_book",
}

type testFunc func(db *sqlx.DB, t *testing.T)

func RunWithScheme(scheme Scheme, t *testing.T, testFn testFunc)  {
	runner := func(db *sqlx.DB, create string, drop string){
		// drop tables
		defer func(){
			db.MustExec(drop)
			db.Close()
		}()
		// prepare environment
		db.MustExec(create)
		// run test function
		testFn(mysqlDB, t)
	}

	if TestMysql {
		create, drop := scheme.Mysql()
		runner(mysqlDB, create, drop)
	}
}

func ConnectAll()  {
	mysqlDNS := os.Getenv("DBUTILS_MYSQL_DNS")

	if TestMysql {
		db, err := sqlx.Open("mysql", mysqlDNS)
		if err != nil {
			fmt.Printf("\nfail to connect mysql, err:%v\n", err)
			TestMysql = false
		}
		mysqlDB = db
	}
}

func TestTables_InsertMap(t *testing.T) {
	RunWithScheme(test_scheme, t, func(sb *sqlx.DB, t *testing.T){
		db := NewDB(sb)
		id, err := db.Tb(t_book).InsertMap(map[string]interface{}{"name": "Python"}).Done()
		if err != nil {
			t.Errorf("got err:%v", err)
		}
		id, err = db.Tb(t_book).InsertMap(map[string]interface{}{"name": "Python"}).Done()
		if err != nil {
			t.Errorf("got err:%v", err)
		}
		if id != 2 {
			t.Logf("expect insert last id 2, got:%d", id)
		}
	})
}

func TestTables_Insert(t *testing.T) {
	RunWithScheme(test_scheme, t, func(sb *sqlx.DB, t *testing.T){
		db := NewDB(sb)
		type Book struct {
			M
			id int64
			Name string `db:"name"`
			Tag int `db:"tag"`
		}
		book := Book{
			Name:"Golang",
			Tag:1,
		}
		id, err := db.Tb(t_book).Insert(&book).Done()
		if id != 1 {
			t.Errorf("expect insert last id as 1, got:%d", id)
		}
		if err != nil {
			t.Errorf("got err:%v", err)
		}
	})
}

func TestSelect_All(t *testing.T) {
	RunWithScheme(test_scheme, t, func(sb *sqlx.DB, t *testing.T){
		db := NewDB(sb)
		type Book struct {
			M
			id int64
			Name string `db:"name"`
			Tag int `db:"tag"`
		}
		set := []*Book{
			&Book{Name:"Python", Tag:99},
			&Book{Name:"Golang", Tag:99},
			&Book{Name:"Tencent", Tag:88},
		}
		for _, book:=range set {
			_, err := db.Tb(t_book).Insert(book).Done()
			if err != nil {
				t.Errorf("fail to insert, err:%v", err)
			}
		}
		var books []Book
		// scan from given columns
		err := db.Tb(t_book).Select("name").Where("tag = ?", 99).OrderDesc("name").All(&books)
		if err != nil {
			t.Errorf("fail to query all, err:%v", err)
		}
		if len(books) != 2 {
			t.Errorf("expect 2, got:%d", len(books))
		}
		if books[0].Name != "Python" {
			t.Errorf("expect got Python, got:%s", books[0].Name)
		}
		if books[0].Tag != 0 {
			t.Errorf("expect no tag value, but got :%d", books[0].Tag)
		}

		books = nil
		// scan from given columns
		err = db.Tb(t_book).Select("tag", "name").Where("tag = ?", 99).OrderDesc("name").All(&books)
		if err != nil {
			t.Errorf("fail to query all, err:%v", err)
		}
		if len(books) != 2 {
			t.Errorf("expect 2, got:%d", len(books))
		}
		if books[0].Name != "Python" {
			t.Errorf("expect got Python, got:%s", books[0].Name)
		}
		if books[0].Tag != 99 {
			t.Errorf("expect tag value 99, but got :%d", books[0].Tag)
		}

		books = nil
		// scan from columns defined in the model struct
		err = db.Tb(t_book).Select().Where("tag = ?", 99).OrderDesc("name").All(&books)
		if err != nil {
			t.Errorf("fail to query all, err:%v", err)
		}
		if len(books) != 2 {
			t.Errorf("expect 2, got:%d", len(books))
		}
		if books[0].Name != "Python" {
			t.Errorf("expect got Python, got:%s", books[0].Name)
		}
		if books[0].Tag != 99 {
			t.Errorf("expect tag value 99, but got :%d", books[0].Tag)
		}
	})
}

func TestSelect_Get(t *testing.T) {
	RunWithScheme(test_scheme, t, func(sb *sqlx.DB, t *testing.T){
		db := NewDB(sb)

		type Book struct {
			M
			id int64
			Name string `db:"name"`
			Tag int `db:"tag"`
		}
		set := []*Book{
			&Book{Name:"Python", Tag:99},
			&Book{Name:"Golang", Tag:99},
			&Book{Name:"Tencent", Tag:88},
		}
		for _, book:=range set {
			_, err := db.Tb(t_book).Insert(book).Done()
			if err != nil {
				t.Errorf("fail to insert, err:%v", err)
			}
		}
		var book Book
		// get exist
		err := db.Tb(t_book).Select().Where("tag = ?", 99).OrderDesc("name").Get(&book)
		if err != nil {
			t.Errorf("fail to query all, err:%v", err)
		}
		if book.Name != "Python" {
			t.Errorf("expect got Python, got:%s", book.Name)
		}
		if book.Tag != 99 {
			t.Errorf("expect tag value 99, but got :%d", book.Tag)
		}

		// get no exists, should got ErrNoRows
		err = db.Tb(t_book).Select().Where("tag = ?", 199).OrderDesc("name").Get(&book)
		if err != sql.ErrNoRows {
			t.Errorf("expect no rows err, got %v", err)
		}

		err = db.Tb(t_book).Select().Where("name IN (?)", "Python").OrderDesc("name").Get(&book)
		if err != nil {
			t.Errorf("expect no err, got %v", err)
		}
	})
}

type tBook struct {
	M
	Id int64
	Name string `db:"name"`
	Tag int `db:"tag"`
}

func (t *tBook) Identity() (string, interface{}) {
	return "name", t.Name
}

func TestDeferWhere(t *testing.T) {
	RunWithScheme(test_scheme, t, func(sb *sqlx.DB, t *testing.T){
		db := NewDB(sb)
		set := []*tBook{
			&tBook{Name:"Python", Tag:99},
			&tBook{Name:"Golang", Tag:99},
			&tBook{Name:"Tencent", Tag:88},
		}
		for _, book:=range set {
			_, err := db.Tb(t_book).Insert(book).Done()
			if err != nil {
				t.Errorf("fail to insert, err:%v", err)
			}
		}
		cnt, err := db.Tb(t_book).Delete().Where("name = ?", "Python").Done()
		if err != nil {
			t.Errorf("got err:%v", err)
		}
		if cnt != 1 {
			t.Errorf("expect delete 1 row, got :%d", cnt)
		}

		cnt, err = db.Tb(t_book).Delete(set[1]).Done()
		if err != nil {
			t.Errorf("got err:%v", err)
		}
		if cnt != 1 {
			t.Errorf("expect delete 1 row, got :%d", cnt)
		}
	})
}
