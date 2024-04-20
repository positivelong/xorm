module xorm.io/xorm

go 1.18

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
	gitee.com/travelliu/dm v1.8.11192
	xorm.io/core v0.0.0-00010101000000-000000000000
)

require (
	cloud.google.com/go v0.37.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.0.0-20190325154230-a5d413f7728c // indirect
	golang.org/x/text v0.3.2 // indirect
	google.golang.org/appengine v1.6.0 // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
)

replace xorm.io/core => ./core
