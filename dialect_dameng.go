// Copyright 2015 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"xorm.io/xorm/core"
	"xorm.io/xorm/util"
)

var (
	damengReservedWords = map[string]bool{
		"ACCESS":                    true,
		"ACCOUNT":                   true,
		"ACTIVATE":                  true,
		"ADD":                       true,
		"ADMIN":                     true,
		"ADVISE":                    true,
		"AFTER":                     true,
		"ALL":                       true,
		"ALL_ROWS":                  true,
		"ALLOCATE":                  true,
		"ALTER":                     true,
		"ANALYZE":                   true,
		"AND":                       true,
		"ANY":                       true,
		"ARCHIVE":                   true,
		"ARCHIVELOG":                true,
		"ARRAY":                     true,
		"AS":                        true,
		"ASC":                       true,
		"AT":                        true,
		"AUDIT":                     true,
		"AUTHENTICATED":             true,
		"AUTHORIZATION":             true,
		"AUTOEXTEND":                true,
		"AUTOMATIC":                 true,
		"BACKUP":                    true,
		"BECOME":                    true,
		"BEFORE":                    true,
		"BEGIN":                     true,
		"BETWEEN":                   true,
		"BFILE":                     true,
		"BITMAP":                    true,
		"BLOB":                      true,
		"BLOCK":                     true,
		"BODY":                      true,
		"BY":                        true,
		"CACHE":                     true,
		"CACHE_INSTANCES":           true,
		"CANCEL":                    true,
		"CASCADE":                   true,
		"CAST":                      true,
		"CFILE":                     true,
		"CHAINED":                   true,
		"CHANGE":                    true,
		"CHAR":                      true,
		"CHAR_CS":                   true,
		"CHARACTER":                 true,
		"CHECK":                     true,
		"CHECKPOINT":                true,
		"CHOOSE":                    true,
		"CHUNK":                     true,
		"CLEAR":                     true,
		"CLOB":                      true,
		"CLONE":                     true,
		"CLOSE":                     true,
		"CLOSE_CACHED_OPEN_CURSORS": true,
		"CLUSTER":                   true,
		"COALESCE":                  true,
		"COLUMN":                    true,
		"COLUMNS":                   true,
		"COMMENT":                   true,
		"COMMIT":                    true,
		"COMMITTED":                 true,
		"COMPATIBILITY":             true,
		"COMPILE":                   true,
		"COMPLETE":                  true,
		"COMPOSITE_LIMIT":           true,
		"COMPRESS":                  true,
		"COMPUTE":                   true,
		"CONNECT":                   true,
		"CONNECT_TIME":              true,
		"CONSTRAINT":                true,
		"CONSTRAINTS":               true,
		"CONTENTS":                  true,
		"CONTINUE":                  true,
		"CONTROLFILE":               true,
		"CONVERT":                   true,
		"COST":                      true,
		"CPU_PER_CALL":              true,
		"CPU_PER_SESSION":           true,
		"CREATE":                    true,
		"CURRENT":                   true,
		"CURRENT_SCHEMA":            true,
		"CURREN_USER":               true,
		"CURSOR":                    true,
		"CYCLE":                     true,
		"DANGLING":                  true,
		"DATABASE":                  true,
		"DATAFILE":                  true,
		"DATAFILES":                 true,
		"DATAOBJNO":                 true,
		"DATE":                      true,
		"DBA":                       true,
		"DBHIGH":                    true,
		"DBLOW":                     true,
		"DBMAC":                     true,
		"DEALLOCATE":                true,
		"DEBUG":                     true,
		"DEC":                       true,
		"DECIMAL":                   true,
		"DECLARE":                   true,
		"DEFAULT":                   true,
		"DEFERRABLE":                true,
		"DEFERRED":                  true,
		"DEGREE":                    true,
		"DELETE":                    true,
		"DEREF":                     true,
		"DESC":                      true,
		"DIRECTORY":                 true,
		"DISABLE":                   true,
		"DISCONNECT":                true,
		"DISMOUNT":                  true,
		"DISTINCT":                  true,
		"DISTRIBUTED":               true,
		"DML":                       true,
		"DOUBLE":                    true,
		"DROP":                      true,
		"DUMP":                      true,
		"EACH":                      true,
		"ELSE":                      true,
		"ENABLE":                    true,
		"END":                       true,
		"ENFORCE":                   true,
		"ENTRY":                     true,
		"ESCAPE":                    true,
		"EXCEPT":                    true,
		"EXCEPTIONS":                true,
		"EXCHANGE":                  true,
		"EXCLUDING":                 true,
		"EXCLUSIVE":                 true,
		"EXECUTE":                   true,
		"EXISTS":                    true,
		"EXPIRE":                    true,
		"EXPLAIN":                   true,
		"EXTENT":                    true,
		"EXTENTS":                   true,
		"EXTERNALLY":                true,
		"FAILED_LOGIN_ATTEMPTS":     true,
		"FALSE":                     true,
		"FAST":                      true,
		"FILE":                      true,
		"FIRST_ROWS":                true,
		"FLAGGER":                   true,
		"FLOAT":                     true,
		"FLOB":                      true,
		"FLUSH":                     true,
		"FOR":                       true,
		"FORCE":                     true,
		"FOREIGN":                   true,
		"FREELIST":                  true,
		"FREELISTS":                 true,
		"FROM":                      true,
		"FULL":                      true,
		"FUNCTION":                  true,
		"GLOBAL":                    true,
		"GLOBALLY":                  true,
		"GLOBAL_NAME":               true,
		"GRANT":                     true,
		"GROUP":                     true,
		"GROUPS":                    true,
		"HASH":                      true,
		"HASHKEYS":                  true,
		"HAVING":                    true,
		"HEADER":                    true,
		"HEAP":                      true,
		"IDENTIFIED":                true,
		"IDGENERATORS":              true,
		"IDLE_TIME":                 true,
		"IF":                        true,
		"IMMEDIATE":                 true,
		"IN":                        true,
		"INCLUDING":                 true,
		"INCREMENT":                 true,
		"INDEX":                     true,
		"INDEXED":                   true,
		"INDEXES":                   true,
		"INDICATOR":                 true,
		"IND_PARTITION":             true,
		"INITIAL":                   true,
		"INITIALLY":                 true,
		"INITRANS":                  true,
		"INSERT":                    true,
		"INSTANCE":                  true,
		"INSTANCES":                 true,
		"INSTEAD":                   true,
		"INT":                       true,
		"INTEGER":                   true,
		"INTERMEDIATE":              true,
		"INTERSECT":                 true,
		"INTO":                      true,
		"IS":                        true,
		"ISOLATION":                 true,
		"ISOLATION_LEVEL":           true,
		"KEEP":                      true,
		"KEY":                       true,
		"KILL":                      true,
		"LABEL":                     true,
		"LAYER":                     true,
		"LESS":                      true,
		"LEVEL":                     true,
		"LIBRARY":                   true,
		"LIKE":                      true,
		"LIMIT":                     true,
		"LINK":                      true,
		"LIST":                      true,
		"LOB":                       true,
		"LOCAL":                     true,
		"LOCK":                      true,
		"LOCKED":                    true,
		"LOG":                       true,
		"LOGFILE":                   true,
		"LOGGING":                   true,
		"LOGICAL_READS_PER_CALL":    true,
		"LOGICAL_READS_PER_SESSION": true,
		"LONG":                      true,
		"MANAGE":                    true,
		"MASTER":                    true,
		"MAX":                       true,
		"MAXARCHLOGS":               true,
		"MAXDATAFILES":              true,
		"MAXEXTENTS":                true,
		"MAXINSTANCES":              true,
		"MAXLOGFILES":               true,
		"MAXLOGHISTORY":             true,
		"MAXLOGMEMBERS":             true,
		"MAXSIZE":                   true,
		"MAXTRANS":                  true,
		"MAXVALUE":                  true,
		"MIN":                       true,
		"MEMBER":                    true,
		"MINIMUM":                   true,
		"MINEXTENTS":                true,
		"MINUS":                     true,
		"MINVALUE":                  true,
		"MLSLABEL":                  true,
		"MLS_LABEL_FORMAT":          true,
		"MODE":                      true,
		"MODIFY":                    true,
		"MOUNT":                     true,
		"MOVE":                      true,
		"MTS_DISPATCHERS":           true,
		"MULTISET":                  true,
		"NATIONAL":                  true,
		"NCHAR":                     true,
		"NCHAR_CS":                  true,
		"NCLOB":                     true,
		"NEEDED":                    true,
		"NESTED":                    true,
		"NETWORK":                   true,
		"NEW":                       true,
		"NEXT":                      true,
		"NOARCHIVELOG":              true,
		"NOAUDIT":                   true,
		"NOCACHE":                   true,
		"NOCOMPRESS":                true,
		"NOCYCLE":                   true,
		"NOFORCE":                   true,
		"NOLOGGING":                 true,
		"NOMAXVALUE":                true,
		"NOMINVALUE":                true,
		"NONE":                      true,
		"NOORDER":                   true,
		"NOOVERRIDE":                true,
		"NOPARALLEL":                true,
		"NOREVERSE":                 true,
		"NORMAL":                    true,
		"NOSORT":                    true,
		"NOT":                       true,
		"NOTHING":                   true,
		"NOWAIT":                    true,
		"NULL":                      true,
		"NUMBER":                    true,
		"NUMERIC":                   true,
		"NVARCHAR2":                 true,
		"OBJECT":                    true,
		"OBJNO":                     true,
		"OBJNO_REUSE":               true,
		"OF":                        true,
		"OFF":                       true,
		"OFFLINE":                   true,
		"OID":                       true,
		"OIDINDEX":                  true,
		"OLD":                       true,
		"ON":                        true,
		"ONLINE":                    true,
		"ONLY":                      true,
		"OPCODE":                    true,
		"OPEN":                      true,
		"OPTIMAL":                   true,
		"OPTIMIZER_GOAL":            true,
		"OPTION":                    true,
		"OR":                        true,
		"ORDER":                     true,
		"ORGANIZATION":              true,
		"OSLABEL":                   true,
		"OVERFLOW":                  true,
		"OWN":                       true,
		"PACKAGE":                   true,
		"PARALLEL":                  true,
		"PARTITION":                 true,
		"PASSWORD":                  true,
		"PASSWORD_GRACE_TIME":       true,
		"PASSWORD_LIFE_TIME":        true,
		"PASSWORD_LOCK_TIME":        true,
		"PASSWORD_REUSE_MAX":        true,
		"PASSWORD_REUSE_TIME":       true,
		"PASSWORD_VERIFY_FUNCTION":  true,
		"PCTFREE":                   true,
		"PCTINCREASE":               true,
		"PCTTHRESHOLD":              true,
		"PCTUSED":                   true,
		"PCTVERSION":                true,
		"PERCENT":                   true,
		"PERMANENT":                 true,
		"PLAN":                      true,
		"PLSQL_DEBUG":               true,
		"POST_TRANSACTION":          true,
		"PRECISION":                 true,
		"PRESERVE":                  true,
		"PRIMARY":                   true,
		"PRIOR":                     true,
		"PRIVATE":                   true,
		"PRIVATE_SGA":               true,
		"PRIVILEGE":                 true,
		"PRIVILEGES":                true,
		"PROCEDURE":                 true,
		"PROFILE":                   true,
		"PUBLIC":                    true,
		"PURGE":                     true,
		"QUEUE":                     true,
		"QUOTA":                     true,
		"RANGE":                     true,
		"RAW":                       true,
		"RBA":                       true,
		"READ":                      true,
		"READUP":                    true,
		"REAL":                      true,
		"REBUILD":                   true,
		"RECOVER":                   true,
		"RECOVERABLE":               true,
		"RECOVERY":                  true,
		"REF":                       true,
		"REFERENCES":                true,
		"REFERENCING":               true,
		"REFRESH":                   true,
		"RENAME":                    true,
		"REPLACE":                   true,
		"RESET":                     true,
		"RESETLOGS":                 true,
		"RESIZE":                    true,
		"RESOURCE":                  true,
		"RESTRICTED":                true,
		"RETURN":                    true,
		"RETURNING":                 true,
		"REUSE":                     true,
		"REVERSE":                   true,
		"REVOKE":                    true,
		"ROLE":                      true,
		"ROLES":                     true,
		"ROLLBACK":                  true,
		"ROW":                       true,
		"ROWID":                     true,
		"ROWNUM":                    true,
		"ROWS":                      true,
		"RULE":                      true,
		"SAMPLE":                    true,
		"SAVEPOINT":                 true,
		"SB4":                       true,
		"SCAN_INSTANCES":            true,
		"SCHEMA":                    true,
		"SCN":                       true,
		"SCOPE":                     true,
		"SD_ALL":                    true,
		"SD_INHIBIT":                true,
		"SD_SHOW":                   true,
		"SEGMENT":                   true,
		"SEG_BLOCK":                 true,
		"SEG_FILE":                  true,
		"SELECT":                    true,
		"SEQUENCE":                  true,
		"SERIALIZABLE":              true,
		"SESSION":                   true,
		"SESSION_CACHED_CURSORS":    true,
		"SESSIONS_PER_USER":         true,
		"SET":                       true,
		"SHARE":                     true,
		"SHARED":                    true,
		"SHARED_POOL":               true,
		"SHRINK":                    true,
		"SIZE":                      true,
		"SKIP":                      true,
		"SKIP_UNUSABLE_INDEXES":     true,
		"SMALLINT":                  true,
		"SNAPSHOT":                  true,
		"SOME":                      true,
		"SORT":                      true,
		"SPECIFICATION":             true,
		"SPLIT":                     true,
		"SQL_TRACE":                 true,
		"STANDBY":                   true,
		"START":                     true,
		"STATEMENT_ID":              true,
		"STATISTICS":                true,
		"STOP":                      true,
		"STORAGE":                   true,
		"STORE":                     true,
		"STRUCTURE":                 true,
		"SUCCESSFUL":                true,
		"SWITCH":                    true,
		"SYS_OP_ENFORCE_NOT_NULL$":  true,
		"SYS_OP_NTCIMG$":            true,
		"SYNONYM":                   true,
		"SYSDATE":                   true,
		"SYSDBA":                    true,
		"SYSOPER":                   true,
		"SYSTEM":                    true,
		"TABLE":                     true,
		"TABLES":                    true,
		"TABLESPACE":                true,
		"TABLESPACE_NO":             true,
		"TABNO":                     true,
		"TEMPORARY":                 true,
		"THAN":                      true,
		"THE":                       true,
		"THEN":                      true,
		"THREAD":                    true,
		"TIMESTAMP":                 true,
		"TIME":                      true,
		"TO":                        true,
		"TOPLEVEL":                  true,
		"TRACE":                     true,
		"TRACING":                   true,
		"TRANSACTION":               true,
		"TRANSITIONAL":              true,
		"TRIGGER":                   true,
		"TRIGGERS":                  true,
		"TRUE":                      true,
		"TRUNCATE":                  true,
		"TX":                        true,
		"TYPE":                      true,
		"UB2":                       true,
		"UBA":                       true,
		"UID":                       true,
		"UNARCHIVED":                true,
		"UNDO":                      true,
		"UNION":                     true,
		"UNIQUE":                    true,
		"UNLIMITED":                 true,
		"UNLOCK":                    true,
		"UNRECOVERABLE":             true,
		"UNTIL":                     true,
		"UNUSABLE":                  true,
		"UNUSED":                    true,
		"UPDATABLE":                 true,
		"UPDATE":                    true,
		"USAGE":                     true,
		"USE":                       true,
		"USER":                      true,
		"USING":                     true,
		"VALIDATE":                  true,
		"VALIDATION":                true,
		"VALUE":                     true,
		"VALUES":                    true,
		"VARCHAR":                   true,
		"VARCHAR2":                  true,
		"VARYING":                   true,
		"VIEW":                      true,
		"WHEN":                      true,
		"WHENEVER":                  true,
		"WHERE":                     true,
		"WITH":                      true,
		"WITHOUT":                   true,
		"WORK":                      true,
		"WRITE":                     true,
		"WRITEDOWN":                 true,
		"WRITEUP":                   true,
		"XID":                       true,
		"YEAR":                      true,
		"ZONE":                      true,
	}
)

type dameng struct {
	core.Base
}

func (db *dameng) Init(d *core.DB, uri *core.Uri, drivername, dataSourceName string) error {
	return db.Base.Init(d, db, uri, drivername, dataSourceName)
}

func (db *dameng) SqlType(c *core.Column) string {
	var res string
	switch t := c.SQLType.Name; t {
	case core.TinyInt, "BYTE":
		return "TINYINT"
	case core.SmallInt, core.MediumInt, core.Int, core.Integer, core.UnsignedTinyInt:
		return "INTEGER"
	case core.BigInt,
		core.UnsignedBigInt, core.UnsignedBit, core.UnsignedInt,
		core.Serial, core.BigSerial:
		return "BIGINT"
	case core.Bit, core.Bool, core.Boolean:
		return core.Bit
	case core.Uuid:
		res = core.Varchar
		c.Length = 40
	case core.Binary:
		if c.Length == 0 {
			return core.Binary + "(MAX)"
		}
	case core.VarBinary, core.Blob, core.TinyBlob, core.MediumBlob, core.LongBlob, core.Bytea:
		return core.VarBinary
	case core.Date:
		return core.Date
	case core.Time:
		if c.Length > 0 {
			return fmt.Sprintf("%s(%d)", core.Time, c.Length)
		}
		return core.Time
	case core.DateTime, core.TimeStamp:
		res = core.TimeStamp
	case core.TimeStampz:
		if c.Length > 0 {
			return fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", c.Length)
		}
		return "TIMESTAMP WITH TIME ZONE"
	case core.Float:
		res = "FLOAT"
	case core.Real, core.Double:
		res = "REAL"
	case core.Numeric, core.Decimal, "NUMBER":
		res = "NUMERIC"
	case core.Text, core.Json:
		return "TEXT"
	case core.MediumText, core.LongText:
		res = "CLOB"
	case core.Char, core.Varchar, core.TinyText:
		res = "VARCHAR2"
	default:
		res = t
	}

	hasLen1 := c.Length > 0
	hasLen2 := c.Length2 > 0

	if hasLen2 {
		res += "(" + strconv.Itoa(c.Length) + "," + strconv.Itoa(c.Length2) + ")"
	} else if hasLen1 {
		res += "(" + strconv.Itoa(c.Length) + ")"
	}
	return res
}

func (db *dameng) SupportInsertMany() bool {
	// todo
	return true
}

func (db *dameng) IsReserved(name string) bool {
	_, ok := damengReservedWords[strings.ToUpper(name)]
	return ok
}

func (db *dameng) Quote(name string) string {
	if strings.ToLower(name) == "login" {
		return `'` + name + `'`
	}
	return `"` + name + `"`
}

func (db *dameng) SupportEngine() bool {
	// todo
	return false
}

func (db *dameng) AutoIncrStr() string {
	return "IDENTITY"
}

func (db *dameng) SupportCharset() bool {
	// todo
	return true
}

func (db *dameng) IndexOnTable() bool {
	return true
}

func (db *dameng) IndexCheckSql(tableName, idxName string) (string, []any) {
	args := []interface{}{db.Schema, tableName, idxName}
	return `select index_name from user_indexes where table_owner = ? and table_name = ? and index_name = ?`, args
}

func (db *dameng) DropIndexSql(tableName string, index *core.Index) string {
	var name string
	if index.IsRegular {
		name = index.XName(tableName)
	} else {
		name = index.Name
	}
	return fmt.Sprintf("DROP INDEX %s.%s", db.Quote(name), db.Quote(tableName))
}

func (db *dameng) TableCheckSql(tableName string) (string, []any) {
	args := []any{tableName, db.Schema}
	sql := "select table_name from all_tables where temporary = 'N' and table_name = ? and owner = ?"
	return sql, args
}

func (db *dameng) GetColumns(tableName string) ([]string, map[string]*core.Column, error) {
	s := `select column_name from user_cons_columns where owner = ? and constraint_name = (select constraint_name from user_constraints where owner = ? and table_name = ? and constraint_type ='P')`
	rows, err := db.DB().Query(s, db.Schema, db.Schema, tableName)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var pkNames []string
	for rows.Next() {
		var pkName string
		err = rows.Scan(&pkName)
		if err != nil {
			return nil, nil, err
		}
		pkNames = append(pkNames, pkName)
	}
	if rows.Err() != nil {
		return nil, nil, rows.Err()
	}
	rows.Close()

	s = `select atc.column_name, atc.data_default, atc.data_type, atc.data_length, atc.data_precision, atc.data_scale, 
atc.nullable, ucc.comments from all_tab_cols as atc left join user_col_comments as ucc on ucc.table_name=atc.table_name 
and ucc.column_name=atc.column_name and atc.owner = ucc.owner where atc.table_name = ? and ucc.owner = ?;`
	rows, err = db.DB().Query(s, tableName, db.Schema)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := make(map[string]*core.Column)
	colSeq := make([]string, 0)
	for rows.Next() {
		col := new(core.Column)
		col.Indexes = make(map[string]int)

		var colDefault dmClobScanner
		var colName, nullable, dataType, dataPrecision, comment sql.NullString
		var dataScale, dataLen sql.NullInt64

		err = rows.Scan(&colName, &colDefault, &dataType, &dataLen, &dataPrecision,
			&dataScale, &nullable, &comment)
		if err != nil {
			return nil, nil, err
		}

		if !colName.Valid {
			return nil, nil, errors.New("column name is nil")
		}

		col.Name = strings.Trim(colName.String, `" `)
		if colDefault.valid {
			col.Default = colDefault.data
		} else {
			col.DefaultIsEmpty = true
		}

		if nullable.String == "Y" {
			col.Nullable = true
		} else {
			col.Nullable = false
		}

		if !comment.Valid {
			col.Comment = comment.String
		}
		if IndexSlice(pkNames, col.Name) > -1 {
			col.IsPrimaryKey = true
			has, err := db.HasRecords("SELECT * FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = ? AND SEQUENCE_NAME = ?", db.Schema, SeqName(tableName))
			if err != nil {
				return nil, nil, err
			}
			if has {
				col.IsAutoIncrement = true
			}
		}

		var (
			ignore     bool
			dt         string
			len1, len2 int
		)

		dts := strings.Split(dataType.String, "(")
		dt = dts[0]
		if len(dts) > 1 {
			lens := strings.Split(dts[1][:len(dts[1])-1], ",")
			if len(lens) > 1 {
				len1, _ = strconv.Atoi(lens[0])
				len2, _ = strconv.Atoi(lens[1])
			} else {
				len1, _ = strconv.Atoi(lens[0])
			}
		}

		switch dt {
		//case :
		//	col.SQLType = core.SQLType{Name: "VARCHAR2", DefaultLength: len1, DefaultLength2: len2}
		case "VARCHAR", "VARCHAR2":
			col.SQLType = core.SQLType{Name: core.Varchar, DefaultLength: len1, DefaultLength2: len2}
		case "TIMESTAMP WITH TIME ZONE":
			col.SQLType = core.SQLType{Name: core.TimeStampz, DefaultLength: 0, DefaultLength2: 0}
		case "NUMBER":
			col.SQLType = core.SQLType{Name: "NUMBER", DefaultLength: len1, DefaultLength2: len2}
		case "LONG", "LONG RAW", "NCLOB", "CLOB", "TEXT":
			col.SQLType = core.SQLType{Name: core.Text, DefaultLength: 0, DefaultLength2: 0}
		case "RAW":
			col.SQLType = core.SQLType{Name: core.Binary, DefaultLength: 0, DefaultLength2: 0}
		case "ROWID":
			col.SQLType = core.SQLType{Name: core.Varchar, DefaultLength: 18, DefaultLength2: 0}
		case "AQ$_SUBSCRIBERS":
			ignore = true
		default:
			col.SQLType = core.SQLType{Name: strings.ToUpper(dt), DefaultLength: len1, DefaultLength2: len2}
		}

		if ignore {
			continue
		}

		if _, ok := core.SqlTypes[col.SQLType.Name]; !ok {
			return nil, nil, fmt.Errorf("unknown colType %v %v", dataType.String, col.SQLType)
		}

		if col.SQLType.Name == "TIMESTAMP" {
			col.Length = int(dataScale.Int64)
		} else {
			col.Length = int(dataLen.Int64)
		}

		if col.SQLType.IsTime() {
			if !col.DefaultIsEmpty && !strings.EqualFold(col.Default, "CURRENT_TIMESTAMP") {
				col.Default = addSingleQuote(col.Default)
			}
		}
		cols[col.Name] = col
		colSeq = append(colSeq, col.Name)
	}
	if rows.Err() != nil {
		return nil, nil, rows.Err()
	}

	return colSeq, cols, nil
}

func (db *dameng) GetTables() ([]*core.Table, error) {
	s := "select table_name from all_tables where owner = ? and temporary = 'N' AND table_name not like ?"
	args := []interface{}{db.Schema, "%$%"}
	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*core.Table, 0)
	for rows.Next() {
		table := core.NewEmptyTable()
		err = rows.Scan(&table.Name)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return tables, nil
}

func (db *dameng) GetIndexes(tableName string) (map[string]*core.Index, error) {
	args := []interface{}{tableName, db.Schema, tableName, db.Schema}
	s := `select t.column_name, i.uniqueness, i.index_name FROM all_ind_columns t left join all_indexes i on 
t.table_name = i.table_name and i.table_owner = t.table_owner and t.index_name = i.index_name WHERE 
t.table_name = ? and i.owner = ? and t.index_name not in (select index_name from all_constraints where 
constraint_type='P' and table_name = ? and owner = ?);`

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]*core.Index)
	for rows.Next() {
		var indexType int
		var indexName, colName, uniqueness string

		err = rows.Scan(&colName, &uniqueness, &indexName)
		if err != nil {
			return nil, err
		}

		indexName = strings.Trim(indexName, `" `)

		var isRegular bool
		if strings.HasPrefix(indexName, "IDX_"+tableName) || strings.HasPrefix(indexName, "UQE_"+tableName) {
			indexName = indexName[5+len(tableName):]
			isRegular = true
		}

		if uniqueness == "UNIQUE" {
			indexType = core.UniqueType
		} else {
			indexType = core.IndexType
		}

		var index *core.Index
		var ok bool
		if index, ok = indexes[indexName]; !ok {
			index = new(core.Index)
			index.Type = indexType
			index.Name = indexName
			index.IsRegular = isRegular
			indexes[indexName] = index
		}
		index.AddColumn(colName)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return indexes, nil
}

func (db *dameng) CreateTableSql(table *core.Table, tableName, storeEngine, charset string) string {
	if tableName == "" {
		tableName = table.Name
	}

	var b strings.Builder
	if _, err := b.WriteString("create table "); err != nil {
		return ""
	}
	if _, err := b.WriteString(db.Quote(tableName)); err != nil {
		return ""
	}
	if _, err := b.WriteString(" ("); err != nil {
		return ""
	}

	pkList := table.PrimaryKeys

	for i, colName := range table.ColumnsSeq() {
		col := table.GetColumn(colName)
		if col.SQLType.IsBool() && !col.DefaultIsEmpty {
			if col.Default == "true" {
				col.Default = "1"
			} else if col.Default == "false" {
				col.Default = "0"
			}
		}

		s, _ := core.ColumnString(db, col, false)
		if _, err := b.WriteString(s); err != nil {
			return ""
		}
		if i != len(table.ColumnsSeq())-1 {
			if _, err := b.WriteString(", "); err != nil {
				return ""
			}
		}
	}

	if len(pkList) > 0 {
		if len(table.ColumnsSeq()) > 0 {
			if _, err := b.WriteString(", "); err != nil {
				return ""
			}
		}
		if _, err := b.WriteString("CONSTRAINT PK_"); err != nil {
			return ""
		}
		if _, err := b.WriteString(tableName); err != nil {
			return ""
		}
		if _, err := b.WriteString(" PRIMARY KEY ("); err != nil {
			return ""
		}
		quoter := util.Quoter{
			Prefix:     '"',
			Suffix:     '"',
			IsReserved: func(string) bool { return true },
		}
		if err := quoter.JoinWrite(&b, pkList, ","); err != nil {
			return ""
		}
		if _, err := b.WriteString(")"); err != nil {
			return ""
		}
	}
	if _, err := b.WriteString(")"); err != nil {
		return ""
	}

	return b.String()
}

func (db *dameng) Filters() []core.Filter {
	return []core.Filter{&core.IdFilter{}}
}

type damengDriver struct {
}

func (p *damengDriver) Parse(driverName, dataSourceName string) (*core.Uri, error) {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, err
	}

	if u.User == nil {
		return nil, errors.New("user/password needed")
	}

	passwd, _ := u.User.Password()
	dbName := u.Query().Get("schema")
	if dbName == "" {
		dbName = u.User.Username()
	}
	return &core.Uri{
		DbType: core.DM,
		Proto:  u.Scheme,
		Host:   u.Hostname(),
		Port:   u.Port(),
		DbName: dbName,
		User:   u.User.Username(),
		Passwd: passwd,
		Schema: dbName,
	}, nil
}

var _ sql.Scanner = &dmClobScanner{}

type dmClobScanner struct {
	valid bool
	data  string
}

type dmClobObject interface {
	GetLength() (int64, error)
	ReadString(int, int) (string, error)
}

func (d *dmClobScanner) Scan(data interface{}) error {
	if data == nil {
		return nil
	}

	switch t := data.(type) {
	case dmClobObject: // *dm.DmClob
		if t == nil {
			return nil
		}
		l, err := t.GetLength()
		if err != nil {
			return err
		}
		if l == 0 {
			d.valid = true
			return nil
		}
		d.data, err = t.ReadString(1, int(l))
		if err != nil {
			return err
		}
		d.valid = true
		return nil
	case []byte:
		if t == nil {
			return nil
		}
		d.data = string(t)
		d.valid = true
		return nil
	case string:
		if len(t) <= 0 {
			return nil
		}
		d.data = t
		d.valid = true
		return nil
	default:
		return fmt.Errorf("cannot convert %T as dmClobScanner", data)
	}
}

// IndexSlice search c in slice s and return the index, return -1 if s don't contain c
func IndexSlice(s []string, c string) int {
	for i, ss := range s {
		if c == ss {
			return i
		}
	}
	return -1
}

func addSingleQuote(name string) string {
	if len(name) < 2 {
		return name
	}
	if name[0] == '\'' && name[len(name)-1] == '\'' {
		return name
	}
	var b strings.Builder
	b.WriteRune('\'')
	b.WriteString(name)
	b.WriteRune('\'')
	return b.String()
}

// SeqName returns sequence name for some table
func SeqName(tableName string) string {
	return "SEQ_" + strings.ToUpper(tableName)
}
