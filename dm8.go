package gorm_dm8

import (
	"database/sql"
	"fmt"
	_ "github.com/xiyichan/dm8"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
	"regexp"
	"strconv"
)

type Config struct {
	DriverName        string
	DSN               string
	DefaultStringSize int
	Conn              gorm.ConnPool
}

type Dialector struct {
	*Config
}

func (d Dialector) Name() string {
	return "dm"
}

func (d Dialector) DummyTableName() string {
	return "DUAL"
}

func (d Dialector) Initialize(db *gorm.DB) (err error) {

	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})

	if d.DriverName == "" {
		d.DriverName = "dm"
	}

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		db.ConnPool, err = sql.Open(d.DriverName, d.DSN)
		if err != nil {
			return err
		}
	}

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					if _, ok := stmt.Clauses["ORDER BY"]; !ok {
						if stmt.Schema != nil && stmt.Schema.PrioritizedPrimaryField != nil {
							builder.WriteString("ORDER BY ")
							builder.WriteQuoted(stmt.Schema.PrioritizedPrimaryField.DBName)
							builder.WriteByte(' ')
						} else {
							builder.WriteString("ORDER BY (SELECT NULL) ")
						}
					}
				}

				if limit.Offset > 0 {
					builder.WriteString("OFFSET ")
					builder.WriteString(strconv.Itoa(limit.Offset))
					builder.WriteString(" ROWS")
				}

				if limit.Limit > 0 {
					if limit.Offset == 0 {
						builder.WriteString("OFFSET 0 ROW")
					}
					builder.WriteString(" FETCH NEXT ")
					builder.WriteString(strconv.Itoa(limit.Limit))
					builder.WriteString(" ROWS ONLY")
				}
			}
		},
	}
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:        db,
				Dialector: d,
			},
		},
		Dialector: d,
	}
}
func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '`':
			continuousBacktick++
			if continuousBacktick == 2 {
				writer.WriteString("\"\"")
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				writer.WriteString("\"")
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				writer.WriteByte('"')
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				writer.WriteString("\"\"")
			}

			writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		writer.WriteString("\"\"")
	}
	writer.WriteString("\"")
}

var numericPlaceholder = regexp.MustCompile("@p(\\d+)")

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (d Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "bit"
	case schema.Int, schema.Uint:
		var sqlType string
		switch {
		case field.Size < 8:
			sqlType = "tinyint"
		case field.Size < 16:
			sqlType = "smallint"
		case field.Size < 31:
			sqlType = "int"
		default:
			sqlType = "bigint"
		}
		if field.AutoIncrement {
			return sqlType + " IDENTITY(1,1)"
		}
		return sqlType
	case schema.Float:
		if field.Precision > 0 {
			return fmt.Sprintf("dec(%d, %d)", field.Precision, field.Scale)
		}

		if field.Size <= 32 {
			return "float"
		}

		return "double"
	case schema.String:
		size := field.Size
		hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
		if (field.PrimaryKey || hasIndex) && size == 0 {
			if d.DefaultStringSize > 0 {
				size = d.DefaultStringSize
			} else {
				size = 256
			}
		}
		if size > 0 && size <= 4000 {
			return fmt.Sprintf("nvarchar(%d)", size)
		}
		return "text"
	case schema.Time:
		precision := ""
		if field.Precision > 0 {
			precision = fmt.Sprintf("(%d)", field.Precision)
		}

		if field.NotNull || field.PrimaryKey {
			return "datetime" + precision
		}
		return "datetime" + precision + " NULL"
	case schema.Bytes:
		if field.Size > 0 && field.Size < 65536 {
			return fmt.Sprintf("binary(%d)", field.Size)
		}
		return "blob"
	}

	return string(field.DataType)
}

func (d Dialector) SavePoint(tx *gorm.DB, name string) error {
	return tx.Exec("SAVEPOINT " + name).Error
}

func (d Dialector) RollbackTo(tx *gorm.DB, name string) error {
	return tx.Exec("ROLLBACK TO SAVEPOINT " + name).Error
}
