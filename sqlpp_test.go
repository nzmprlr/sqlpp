package sqlpp

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

var errPrepareNotSupported = errors.New(mysqlErrPrefixPrepareNotSupported)

func TestDB_transform(t *testing.T) {
	cases := []struct {
		query     string
		args      []interface{}
		eSqlQuery string
		ePgQuery  string
		eArgs     []interface{}
	}{
		{
			"select * from foo", nil,
			"select * from foo",
			"select * from foo",
			nil,
		}, {
			"select * from foo where i in (?)", nil,
			"select * from foo where i in ",
			"select * from foo where i in ",
			[]interface{}{},
		}, {
			"select * from foo where i in (?)", []interface{}{[]int{}},
			"select * from foo where i in (?)",
			"select * from foo where i in ($1)",
			[]interface{}{},
		}, {
			"select a,b from foo where i in (?)", []interface{}{[]int{1, 2}},
			"select a,b from foo where i in (?,?)",
			"select a,b from foo where i in ($1,$2)",
			[]interface{}{1, 2},
		}, {
			"select a,b from foo where i = ? and j in (?) or k = ?", []interface{}{"i", []int{1, 2}, "k"},
			"select a,b from foo where i = ? and j in (?,?) or k = ?",
			"select a,b from foo where i = $1 and j in ($2,$3) or k = $4",
			[]interface{}{"i", 1, 2, "k"},
		}, {
			"select a,b from foo where i = ? and j in (?) or k = ? and l in (?)", []interface{}{"i", []int{1, 2, 3, 4, 5}, "k", []string{"str", "ing"}},
			"select a,b from foo where i = ? and j in (?,?,?,?,?) or k = ? and l in (?,?)",
			"select a,b from foo where i = $1 and j in ($2,$3,$4,$5,$6) or k = $7 and l in ($8,$9)",
			[]interface{}{"i", 1, 2, 3, 4, 5, "k", "str", "ing"},
		}, {
			"select * from foo where i in (?) and j in (?)", []interface{}{[]int64{-123, 123}, []string{"str", "ing"}},
			"select * from foo where i in (?,?) and j in (?,?)",
			"select * from foo where i in ($1,$2) and j in ($3,$4)",
			[]interface{}{int64(-123), int64(123), "str", "ing"},
		}, {
			"select * from foo where (i = ? and j in (?)) or (k = ? and l in (?)) or m in (?)", []interface{}{"i", []int{1}, "k", []string{"str", "ing"}, []uint16{1, 2, 3}},
			"select * from foo where (i = ? and j in (?)) or (k = ? and l in (?,?)) or m in (?,?,?)",
			"select * from foo where (i = $1 and j in ($2)) or (k = $3 and l in ($4,$5)) or m in ($6,$7,$8)",
			[]interface{}{"i", 1, "k", "str", "ing", uint16(1), uint16(2), uint16(3)},
		}, {
			"select koo.bar from foo koo inner join loo moo on koo.bar = moo.baz where koo.bar in (?) order by 1", []interface{}{[]int{1, 2, 3}},
			"select koo.bar from foo koo inner join loo moo on koo.bar = moo.baz where koo.bar in (?,?,?) order by 1",
			"select koo.bar from foo koo inner join loo moo on koo.bar = moo.baz where koo.bar in ($1,$2,$3) order by 1",
			[]interface{}{1, 2, 3},
		},
	}

	t.Parallel()
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s#%+v", c.query, c.args), func(t *testing.T) {
			m := NewMySQL(nil)
			p := NewPostgreSQL(nil)

			meq, mea := m.transform(c.query, c.args)
			peq, pea := p.transform(c.query, c.args)

			assert.Equal(t, meq, c.eSqlQuery)
			assert.Equal(t, peq, c.ePgQuery)

			assert.Equal(t, mea, c.eArgs)
			assert.Equal(t, pea, c.eArgs)
		})
	}
}

func TestDB_prepare(t *testing.T) {
	mDb, mMock, mErr := sqlmock.New()
	pDb, pMock, pErr := sqlmock.New()
	assert.Nil(t, mErr)
	assert.Nil(t, pErr)

	sm := NewMySQL(mDb)
	sp := NewPostgreSQL(pDb)

	cases := []struct {
		cached bool
		query  string
		eQuery string
		err    bool
	}{
		{
			// cache foo stmt
			false,
			"select * from foo",
			"^select (.+) from foo$",
			false,
		},
		{
			// get foo from cache
			true,
			"select * from foo",
			"^select (.+) from foo$",
			false,
		}, {
			// cache bar err
			false,
			"select * from bar",
			"^select (.+) from bar$",
			true,
		}, {
			// get bar err cache
			true,
			"select * from bar",
			"^select (.+) from bar$",
			true,
		},
	}

	t.Parallel()
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s#%t", c.query, c.err), func(t *testing.T) {
			if !c.cached {
				m := mMock.ExpectPrepare(c.eQuery)
				p := pMock.ExpectPrepare(c.eQuery)

				if c.err {
					m.WillReturnError(errPrepareNotSupported)
					p.WillReturnError(errPrepareNotSupported)
				}
			}

			mStmt, _, _, mErr := sm.prepare(context.Background(), c.query, nil)
			pStmt, _, _, pErr := sp.prepare(context.Background(), c.query, nil)

			if c.err {
				assert.Nil(t, mStmt)
				assert.Nil(t, pStmt)
				assert.True(t, isMysqlPrepareNotSupported(mErr))
				assert.True(t, isMysqlPrepareNotSupported(pErr))
			} else {
				assert.NotNil(t, mStmt)
				assert.NotNil(t, pStmt)
				assert.Nil(t, mErr)
				assert.Nil(t, pErr)
			}
		})
	}

	assert.Nil(t, mMock.ExpectationsWereMet())
	assert.Nil(t, pMock.ExpectationsWereMet())
}

func TestDB_Close(t *testing.T) {
	mDb, mMock, mErr := sqlmock.New()
	pDb, pMock, pErr := sqlmock.New()
	assert.Nil(t, mErr)
	assert.Nil(t, pErr)

	sm := NewMySQL(mDb)
	sp := NewPostgreSQL(pDb)

	cases := []struct {
		query  string
		eQuery string
		err    bool
	}{
		{
			"select * from foo",
			"^select (.+) from foo$",
			false,
		}, {
			"select * from bar",
			"^select (.+) from bar$",
			false,
		}, {
			"select * from baz",
			"^select (.+) from baz$",
			true,
		},
	}

	for _, c := range cases {
		mm := mMock.ExpectPrepare(c.eQuery)
		mp := pMock.ExpectPrepare(c.eQuery)

		if c.err {
			mm.WillReturnError(errPrepareNotSupported)
			mp.WillReturnError(errPrepareNotSupported)
		} else {
			mm.WillBeClosed()
			mp.WillBeClosed()
		}

		sm.prepare(context.Background(), c.query, nil)
		sp.prepare(context.Background(), c.query, nil)
	}

	assertLen := func(s, e int) {
		len := func(m sync.Map) (int, int, int) {
			ls := 0
			le := 0
			lu := 0
			m.Range(func(key, value interface{}) bool {
				if _, o := value.(*sql.Stmt); o {
					ls++
				} else if _, o := value.(error); o {
					le++
				} else {
					lu++
				}

				return true
			})

			return ls, le, lu
		}

		mls, mle, mlu := len(sm.stmts)
		pls, ple, plu := len(sp.stmts)

		assert.Equal(t, mls, pls)
		assert.Equal(t, mle, ple)
		assert.Equal(t, mlu, plu)
		assert.Equal(t, mls, s)
		assert.Equal(t, mle, e)
		assert.Equal(t, mlu, 0)
	}

	assertLen(2, 1)

	mMock.ExpectClose()
	pMock.ExpectClose()

	sm.Close()
	sp.Close()

	assertLen(0, 0)

	assert.Nil(t, mMock.ExpectationsWereMet())
	assert.Nil(t, pMock.ExpectationsWereMet())
}

func TestDB_Exec(t *testing.T) {
	mDb, mMock, mErr := sqlmock.New()
	pDb, pMock, pErr := sqlmock.New()
	assert.Nil(t, mErr)
	assert.Nil(t, pErr)

	sm := NewMySQL(mDb)
	sp := NewPostgreSQL(pDb)

	cases := []struct {
		query      string
		eQuery     string
		args       []interface{}
		eArgs      []driver.Value
		prepareErr error
		directExec bool
	}{
		{
			"select * from foo where i in (?)",
			"^select (.+) from foo where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			nil,
			false,
		}, {
			"select * from bar where i in (?)",
			"^select (.+) from bar where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			errPrepareNotSupported,
			true,
		}, {
			"select * from baz where i in (?)",
			"^select (.+) from baz where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			errors.New("error"),
			false,
		},
	}

	for _, c := range cases {
		mm := mMock.ExpectPrepare(c.eQuery)
		pm := pMock.ExpectPrepare(c.eQuery)

		if c.prepareErr == nil {
			mm.ExpectExec().WithArgs(c.eArgs...)
			pm.ExpectExec().WithArgs(c.eArgs...)
		} else {
			mm.WillReturnError(c.prepareErr)
			pm.WillReturnError(c.prepareErr)

			if c.directExec {
				mMock.ExpectExec(c.eQuery).WithArgs(c.eArgs...)
				pMock.ExpectExec(c.eQuery).WithArgs(c.eArgs...)
			}
		}

		sm.Exec(c.query, c.args...)
		sp.Exec(c.query, c.args...)
	}

	assert.Nil(t, mMock.ExpectationsWereMet())
	assert.Nil(t, pMock.ExpectationsWereMet())
}

func TestDB_QueryRow(t *testing.T) {
	mDb, mMock, mErr := sqlmock.New()
	pDb, pMock, pErr := sqlmock.New()
	assert.Nil(t, mErr)
	assert.Nil(t, pErr)

	sm := NewMySQL(mDb)
	sp := NewPostgreSQL(pDb)

	cases := []struct {
		query      string
		eQuery     string
		args       []interface{}
		eArgs      []driver.Value
		eReturn    int
		prepareErr error
		directExec bool
		execErr    error
	}{
		{
			"select * from foo where i in (?)",
			"^select (.+) from foo where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			1,
			nil,
			false,
			nil,
		}, {
			"select * from koo where i in (?)",
			"^select (.+) from koo where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			1,
			nil,
			false,
			errors.New("exec err"),
		}, {
			"select * from bar where i in (?)",
			"^select (.+) from bar where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			2,
			errPrepareNotSupported,
			true,
			nil,
		}, {
			"select * from baz where i in (?)",
			"^select (.+) from baz where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			3,
			errors.New("error"),
			false,
			nil,
		},
	}

	for _, c := range cases {
		mm := mMock.ExpectPrepare(c.eQuery)
		pm := pMock.ExpectPrepare(c.eQuery)

		expectReturn := false
		if c.prepareErr == nil {
			emm := mm.ExpectQuery().WithArgs(c.eArgs...)
			epm := pm.ExpectQuery().WithArgs(c.eArgs...)

			if c.execErr == nil {
				emm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn))
				epm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn))
				expectReturn = true
			} else {
				emm.WillReturnError(c.execErr)
				epm.WillReturnError(c.execErr)
			}
		} else {
			mm.WillReturnError(c.prepareErr)
			pm.WillReturnError(c.prepareErr)

			if c.directExec {
				mm := mMock.ExpectQuery(c.eQuery).WithArgs(c.eArgs...)
				pm := pMock.ExpectQuery(c.eQuery).WithArgs(c.eArgs...)
				if c.execErr == nil {
					mm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn))
					pm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn))
					expectReturn = true
				} else {
					mm.WillReturnError(c.execErr)
					pm.WillReturnError(c.execErr)
				}
			}
		}

		var im, ip int
		em := sm.QueryRow(c.query, sm.Args(c.args...), &im)
		ep := sp.QueryRow(c.query, sp.Args(c.args...), &ip)

		if expectReturn {
			assert.Equal(t, im, c.eReturn)
			assert.Equal(t, ip, c.eReturn)
		}

		if c.prepareErr != nil && !expectReturn {
			assert.Equal(t, em, c.prepareErr)
			assert.Equal(t, ep, c.prepareErr)
		} else if c.execErr != nil {
			assert.Equal(t, em, c.execErr)
			assert.Equal(t, ep, c.execErr)
		} else {
			assert.Nil(t, em)
			assert.Nil(t, ep)
		}
	}

	assert.Nil(t, mMock.ExpectationsWereMet())
	assert.Nil(t, pMock.ExpectationsWereMet())
}

func TestDB_Query(t *testing.T) {
	mDb, mMock, mErr := sqlmock.New()
	pDb, pMock, pErr := sqlmock.New()
	assert.Nil(t, mErr)
	assert.Nil(t, pErr)

	sm := NewMySQL(mDb)
	sp := NewPostgreSQL(pDb)

	cases := []struct {
		query      string
		eQuery     string
		args       []interface{}
		eArgs      []driver.Value
		eReturn    []driver.Value
		prepareErr error
		directExec bool
		execErr    error
	}{
		{
			"select * from foo where i in (?)",
			"^select (.+) from foo where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			[]driver.Value{1},
			nil,
			false,
			nil,
		}, {
			"select * from koo where i in (?)",
			"^select (.+) from koo where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			[]driver.Value{1},
			nil,
			false,
			errors.New("exec err"),
		}, {
			"select * from bar where i in (?)",
			"^select (.+) from bar where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			[]driver.Value{2},
			errPrepareNotSupported,
			true,
			nil,
		}, {
			"select * from baz where i in (?)",
			"^select (.+) from baz where i in (.+)$",
			[]interface{}{[]int{1, 2}},
			[]driver.Value{1, 2},
			[]driver.Value{3},
			errors.New("error"),
			false,
			nil,
		},
	}

	for _, c := range cases {
		mm := mMock.ExpectPrepare(c.eQuery)
		pm := pMock.ExpectPrepare(c.eQuery)

		expectReturn := false
		if c.prepareErr == nil {
			emm := mm.ExpectQuery().WithArgs(c.eArgs...)
			epm := pm.ExpectQuery().WithArgs(c.eArgs...)

			if c.execErr == nil {
				emm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn...)).RowsWillBeClosed()
				epm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn...)).RowsWillBeClosed()
				expectReturn = true
			} else {
				emm.WillReturnError(c.execErr)
				epm.WillReturnError(c.execErr)
			}
		} else {
			mm.WillReturnError(c.prepareErr)
			pm.WillReturnError(c.prepareErr)

			if c.directExec {
				mm := mMock.ExpectQuery(c.eQuery).WithArgs(c.eArgs...)
				pm := pMock.ExpectQuery(c.eQuery).WithArgs(c.eArgs...)
				if c.execErr == nil {
					mm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn...)).RowsWillBeClosed()
					pm.WillReturnRows(sqlmock.NewRows([]string{"return"}).AddRow(c.eReturn...)).RowsWillBeClosed()
					expectReturn = true
				} else {
					mm.WillReturnError(c.execErr)
					pm.WillReturnError(c.execErr)
				}
			}
		}

		scanner := func(r *sql.Rows) (interface{}, error) {
			var i int
			return i, r.Scan(&i)
		}

		convert := func(values []interface{}) []driver.Value {
			r := make([]driver.Value, len(values))
			for i, v := range values {
				r[i] = driver.Value(v)
			}

			return r
		}

		rm, em := sm.Query(c.query, sm.Args(c.args...), scanner)
		rp, ep := sp.Query(c.query, sp.Args(c.args...), scanner)

		if expectReturn {
			assert.Equal(t, rm, rp)
			assert.Equal(t, convert(rm), c.eReturn)
		}

		if c.prepareErr != nil && !expectReturn {
			assert.Equal(t, em, c.prepareErr)
			assert.Equal(t, ep, c.prepareErr)
		} else if c.execErr != nil {
			assert.Equal(t, em, c.execErr)
			assert.Equal(t, ep, c.execErr)
		} else {
			assert.Nil(t, em)
			assert.Nil(t, ep)
		}
	}

	assert.Nil(t, mMock.ExpectationsWereMet())
	assert.Nil(t, pMock.ExpectationsWereMet())
}
