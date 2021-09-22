# sqlpp [![GoDoc](https://godoc.org/github.com/nzmprlr/sqlpp?status.svg)](http://godoc.org/github.com/nzmprlr/sqlpp) [![Go Report Card](https://goreportcard.com/badge/github.com/nzmprlr/sqlpp)](https://goreportcard.com/report/github.com/nzmprlr/sqlpp) [![Coverage](http://gocover.io/_badge/github.com/nzmprlr/sqlpp)](http://gocover.io/github.com/nzmprlr/sqlpp)

sqlpp is a sql(`MySQL and PostgreSQL`) database connection wrapper to cache prepared statements by transforming queries (`"...in (?)...", []`) to use with array arguments.

## Query Transformation
### Given query:
 `select * from bar where b = ? or a in (?) or b = ? or b in (?)` 
 ### With args: 
 `db.Args(1, []int{2,3}, 4, []string{"5", "6", "7"})` 
 
 ### Will transform to:
 MySQL => `select * from bar where b = ? or a in (?,?) or b = ? or b in (?,?,?)`<br> PostgreSQL => `select * from bar where b = $1 or a in ($2,$3) or b = $4 or b in ($5,$6,$7)`
 ### With args:
 `[]interface{}{1, 2, 3, 4, "5", "6", "7"}`
<br>
## Usage

``` go
/* conn, _ := sql.Open("mysql", "username:password@tcp(host:port)/database")
db := sqlpp.NewMySQL(conn) */
conn, _ := sql.Open("postgres", "postgres://username:password@host:port/database?sslmode=disable")
db := sqlpp.NewPostgreSQL(conn)

defer db.Close()

err = db.Ping()
if err != nil {
    panic(err)
}

r, _ := db.Query("select * from foo", nil, func(r *sql.Rows) (interface{}, error) {
    var a int
    err := r.Scan(&a)
    return a, err
})

fmt.Println(r)
// output: [1,2,3,4]

r, _ = db.Query("select * from foo where id = ?", db.Args(1), func(r *sql.Rows) (interface{}, error) {
    var a int
    err := r.Scan(&a)
    return a, err
})

fmt.Println(r)
// output: [1]


r, _ = db.Query("select * from foo where id in (?)", db.Args([]int{2,3}), func(r *sql.Rows) (interface{}, error) {
    var a int
    err := r.Scan(&a)
    return a, err
})

fmt.Println(r)
// output: [2,3]
```

## License

The MIT License (MIT). See [License File](LICENSE) for more information.
