package om

import (
	"testing"
	"os"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql"
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

func TestTables_Insert(t *testing.T) {
	RunWithScheme(test_scheme, t, func(sb *sqlx.DB, t *testing.T){
		db := NewDB(sb)
		id, err := db.Tb(t_book).Insert(map[string]interface{}{"name": "Python"}).Done()
		if err != nil {
			t.Errorf("got err:%v", err)
		}
		id, err = db.Tb(t_book).Insert(map[string]interface{}{"name": "Python"}).Done()
		if err != nil {
			t.Errorf("got err:%v", err)
		}
		if id != 2 {
			t.Logf("expect insert last id 2, got:%d", id)
		}
	})
}

