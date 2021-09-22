package sqlpp

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrNilRows    = errors.New("sqlpp: nil rows")
	ErrNilScanner = errors.New("sqlpp: nil scanner")
)

func NewPostgreSQL(db *sql.DB) *DB {
	return new(db, true)
}

func NewMySQL(db *sql.DB) *DB {
	return new(db, false)
}

func new(db *sql.DB, postgres bool) *DB {
	return &DB{
		DB:       db,
		postgres: postgres,

		stmts: sync.Map{},
	}
}

type DB struct {
	*sql.DB

	postgres bool

	// stmt cache
	stmts sync.Map
}

func (sqlpp *DB) transform(query string, args []interface{}) (string, []interface{}) {
	if i := strings.LastIndex(query, "(?)"); i != -1 {
		indices := []int{}
		tempQuery := query
		for ; i != -1; i = strings.LastIndex(tempQuery, "(?)") {
			indices = append(indices, i)
			tempQuery = tempQuery[:i]
		}

		lenIndices := len(indices)
		tempArgs := []interface{}{}
		for _, arg := range args {
			switch reflect.TypeOf(arg).Kind() {
			case reflect.Array, reflect.Slice:
				v := reflect.ValueOf(arg)
				l := v.Len()
				if l == 0 {
					tempQuery += "(?)"
				} else {
					tempQuery += "(" + strings.Repeat("?,", l)[:l*2-1] + ")"
				}

				if lenIndices--; lenIndices > 0 {
					tempQuery += query[indices[lenIndices]+3 : indices[lenIndices-1]]
				} else {
					tempQuery += query[indices[0]+3:]
				}

				for i := 0; i < l; i++ {
					tempArgs = append(tempArgs, v.Index(i).Interface())
				}

			default:
				tempArgs = append(tempArgs, arg)
			}
		}

		query = tempQuery
		args = tempArgs
	}

	if sqlpp.postgres {
		count := strings.Count(query, "?")
		for i := 1; i <= count; i++ {
			query = strings.Replace(query, "?", "$"+strconv.Itoa(i), 1)
		}
	}

	return query, args
}

func (sqlpp *DB) prepare(ctx context.Context, query string, args []interface{}) (*sql.Stmt, string, []interface{}, error) {
	query, args = sqlpp.transform(query, args)

	if loaded, ok := sqlpp.stmts.Load(query); ok {
		if stmt, o := loaded.(*sql.Stmt); o {
			return stmt, query, args, nil
		} else if err, o := loaded.(error); o {
			return nil, query, args, err
		} else {
			sqlpp.stmts.Delete(query)
		}
	}

	stmt, err := sqlpp.PrepareContext(ctx, query)
	if err != nil {
		if isMysqlPrepareNotSupported(err) {
			sqlpp.stmts.Store(query, err)
		}

		return nil, query, args, err
	}

	sqlpp.stmts.Store(query, stmt)
	return stmt, query, args, nil
}

type Scanner func(*sql.Rows) (interface{}, error)

func (sqlpp *DB) parse(rows *sql.Rows, scanner Scanner) ([]interface{}, error) {
	if rows == nil {
		return nil, ErrNilRows
	} else if scanner == nil {
		return nil, ErrNilScanner
	}

	results := []interface{}{}
	for rows.Next() {
		scanned, err := scanner(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}

		results = append(results, scanned)
	}

	return results, nil
}

func (sqlpp *DB) Args(args ...interface{}) []interface{} {
	return args
}

func (sqlpp *DB) Close() error {
	sqlpp.stmts.Range(func(key, value interface{}) bool {
		if stmt, o := value.(*sql.Stmt); o {
			stmt.Close()
		}

		return true
	})

	sqlpp.stmts = sync.Map{}
	return sqlpp.DB.Close()
}

func (sqlpp *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return sqlpp.ExecContext(context.Background(), query, args...)
}
func (sqlpp *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, query, args, err := sqlpp.prepare(ctx, query, args)
	if err != nil {
		if isMysqlPrepareNotSupported(err) {
			return sqlpp.DB.ExecContext(ctx, query, args...)
		}

		return nil, err
	}

	return stmt.ExecContext(ctx, args...)
}

func (sqlpp *DB) QueryRow(query string, args []interface{}, dest ...interface{}) error {
	return sqlpp.QueryRowContext(context.Background(), query, args, dest...)
}
func (sqlpp *DB) QueryRowContext(ctx context.Context, query string, args []interface{}, dest ...interface{}) error {
	stmt, query, args, err := sqlpp.prepare(ctx, query, args)
	if err != nil {
		if isMysqlPrepareNotSupported(err) {
			err = sqlpp.DB.QueryRowContext(ctx, query, args...).Scan(dest...)
		}

		return err
	}

	return stmt.QueryRowContext(ctx, args...).Scan(dest...)
}

func (sqlpp *DB) Query(query string, args []interface{}, scan Scanner) ([]interface{}, error) {
	return sqlpp.QueryContext(context.Background(), query, args, scan)
}
func (sqlpp *DB) QueryContext(ctx context.Context, query string, args []interface{}, scan Scanner) ([]interface{}, error) {
	var rows *sql.Rows
	stmt, query, args, err := sqlpp.prepare(ctx, query, args)
	if err != nil {
		if isMysqlPrepareNotSupported(err) {
			rows, err = sqlpp.DB.QueryContext(ctx, query, args...)
		} else {
			return nil, err
		}
	} else {
		rows, err = stmt.QueryContext(ctx, args...)
	}

	if err != nil {
		return nil, err
	}

	return sqlpp.parse(rows, scan)
}
