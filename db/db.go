package db

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	// Driver for database/sql package
	_ "github.com/go-sql-driver/mysql"
	migrate "github.com/rubenv/sql-migrate"
)

// DB is main class for MySQL API
type DB struct {
	con   *sql.DB
	mutex *sync.RWMutex
}

func checkEnv() bool {
	if os.Getenv("DB_USER") == "" ||
		os.Getenv("DB_PASS") == "" ||
		os.Getenv("DB_HOST") == "" ||
		os.Getenv("DB_PORT") == "" ||
		os.Getenv("DB_NAME") == "" {
		return false
	}

	return true
}

func dbDestructor(db *DB) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if db.con != nil {
		db.con.Close()
		db.con = nil
	}
}

// New is function for construct a new DB connection
// arg[0] - DB_USER
// arg[1] - DB_PASS
// arg[2] - DB_HOST
// arg[3] - DB_PORT
// arg[4] - DB_NAME
func New(args ...string) (*DB, error) {
	var addr string
	if len(args) == 5 {
		addr = fmt.Sprintf("%s:%s@%s/%s?parseTime=true", args[0], args[1],
			fmt.Sprintf("tcp(%s:%s)", args[2], args[3]), args[4])
	} else if checkEnv() {
		addr = fmt.Sprintf("%s:%s@%s/%s?parseTime=true", os.Getenv("DB_USER"),
			os.Getenv("DB_PASS"), fmt.Sprintf("tcp(%s:%s)",
				os.Getenv("DB_HOST"), os.Getenv("DB_PORT")), os.Getenv("DB_NAME"))
	} else {
		return nil, errors.New("DB initialization params is not defined")
	}

	con, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, err
	}
	con.SetMaxIdleConns(3)
	con.SetMaxOpenConns(256)

	db := &DB{con: con, mutex: &sync.RWMutex{}}
	runtime.SetFinalizer(db, dbDestructor)

	return db, nil
}

// Query is function for execute select query type
func (db *DB) Query(query string, args ...interface{}) (result []map[string]string, err error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.con == nil {
		err = errors.New("database connection is already released")
		return
	}

	result = make([]map[string]string, 0)
	rows, err := db.con.Query(query, args...)
	if err != nil {
		return
	}

	defer rows.Close()
	columnNames, err := rows.Columns()
	if err != nil {
		return
	}

	vals := make([]interface{}, len(columnNames))
	for rows.Next() {
		for i := range columnNames {
			vals[i] = &vals[i]
		}
		err = rows.Scan(vals...)
		if err != nil {
			return
		}
		var row = make(map[string]string)
		for i := range columnNames {
			switch vals[i].(type) {
			case int, int64:
				row[columnNames[i]] = fmt.Sprintf("%d", vals[i])
			case float32, float64:
				row[columnNames[i]] = fmt.Sprintf("%f", vals[i])
			case nil:
				row[columnNames[i]] = ""
			default:
				row[columnNames[i]] = fmt.Sprintf("%s", vals[i])
			}
		}
		result = append(result, row)
	}

	return
}

// Exec is function for execute update and delete query type
func (db *DB) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.con == nil {
		err = errors.New("database connection is already released")
		return
	}

	result, err = db.con.Exec(query, args...)

	return
}

// MigrateUp is function that receives path to migration dir and runs up ones
func (db *DB) MigrateUp(path string) (err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.con == nil {
		err = errors.New("database connection is already released")
		return
	}

	migrations := &migrate.FileMigrationSource{
		Dir: path,
	}
	_, err = migrate.Exec(db.con, "mysql", migrations, migrate.Up)

	return
}

// MigrateDown is function that receives path to migration dir and runs down ones
func (db *DB) MigrateDown(path string) (err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.con == nil {
		err = errors.New("database connection is already released")
		return
	}

	migrations := &migrate.FileMigrationSource{
		Dir: path,
	}
	_, err = migrate.Exec(db.con, "mysql", migrations, migrate.Down)

	return
}
