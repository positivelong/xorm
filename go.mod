module xorm.io/xorm

go 1.11

require (
	github.com/denisenkom/go-mssqldb v0.0.0-20190707035753-2be1aa521ff4
	github.com/go-sql-driver/mysql v1.4.1
	github.com/kr/pretty v0.1.0 // indirect
	github.com/lib/pq v1.0.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/stretchr/testify v1.4.0
	github.com/ziutek/mymysql v1.5.4
	xorm.io/builder v0.3.6
)

require (
	xorm.io/core v0.0.0-00010101000000-000000000000
)

replace (
	xorm.io/core => ./core
)