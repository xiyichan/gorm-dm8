package gorm_dm8

import (
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("SELECT SYS_CONTEXT ('userenv', 'current_schema') FROM DUAL").Row().Scan(&name)
	return
}

//func (m Migrator) FullDataTypeOf(field *schema.Field) clause.Expr {
//	expr := m.Migrator.FullDataTypeOf(field)
//
//	if value, ok := field.TagSettings["COMMENT"]; ok {
//		expr.SQL += " COMMENT " + m.Dialector.Explain("?", value)
//	}
//
//	return expr
//}
func (m Migrator) AddColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? ADD ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, m.DB.Migrator().FullDataTypeOf(field),
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}
func (m Migrator) DropColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(name); field != nil {
			name = field.DBName
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP COLUMN ?", m.CurrentTable(stmt), clause.Column{Name: name},
		).Error
	})
}
func (m Migrator) AlterColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? MODIFY COLUMN ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, m.FullDataTypeOf(field),
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(oldName); field != nil {
			oldName = field.DBName
		}

		if field := stmt.Schema.LookUpField(newName); field != nil {
			newName = field.DBName
		}

		return m.DB.Exec(
			"ALTER TABLE ? RENAME COLUMN ? TO ?",
			m.CurrentTable(stmt), clause.Column{Name: oldName}, clause.Column{Name: newName},
		).Error
	})
}

//type BuildIndexOptionsInterface interface {
//	BuildIndexOptions([]schema.IndexOption, *gorm.Statement) []interface{}
//}

//func (m Migrator) CreateTable(values ...interface{}) error {
//	for _, value := range m.ReorderModels(values, false) {
//		tx := m.DB.Session(&gorm.Session{})
//		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (errr error) {
//			var (
//				createTableSQL          = "CREATE TABLE ? ("
//				values                  = []interface{}{m.CurrentTable(stmt)}
//				hasPrimaryKeyInDataType bool
//			)
//
//			for _, dbName := range stmt.Schema.DBNames {
//				field := stmt.Schema.FieldsByDBName[dbName]
//				if !field.IgnoreMigration {
//					createTableSQL += "? ?"
//					hasPrimaryKeyInDataType = hasPrimaryKeyInDataType || strings.Contains(strings.ToUpper(string(field.DataType)), "PRIMARY KEY")
//					values = append(values, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(field))
//					createTableSQL += ","
//				}
//			}
//
//			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
//				createTableSQL += "PRIMARY KEY ?,"
//				primaryKeys := []interface{}{}
//				for _, field := range stmt.Schema.PrimaryFields {
//					primaryKeys = append(primaryKeys, clause.Column{Name: field.DBName})
//				}
//
//				values = append(values, primaryKeys)
//			}
//
//			for _, idx := range stmt.Schema.ParseIndexes() {
//				if m.CreateIndexAfterCreateTable {
//					defer func(value interface{}, name string) {
//						if errr == nil {
//							errr = tx.Migrator().CreateIndex(value, name)
//						}
//					}(value, idx.Name)
//				} else {
//					if idx.Class != "" {
//						createTableSQL += idx.Class + " "
//					}
//					createTableSQL += "INDEX ? ?"
//
//					if idx.Comment != "" {
//						createTableSQL += fmt.Sprintf(" COMMENT '%s'", idx.Comment)
//					}
//
//					if idx.Option != "" {
//						createTableSQL += " " + idx.Option
//					}
//
//					createTableSQL += ","
//					values = append(values, clause.Expr{SQL: idx.Name}, tx.Migrator().(BuildIndexOptionsInterface).BuildIndexOptions(idx.Fields, stmt))
//				}
//			}
//
//			for _, rel := range stmt.Schema.Relationships.Relations {
//				if !m.DB.DisableForeignKeyConstraintWhenMigrating {
//					if constraint := rel.ParseConstraint(); constraint != nil {
//						if constraint.Schema == stmt.Schema {
//							sql, vars := buildConstraint(constraint)
//							createTableSQL += sql + ","
//							values = append(values, vars...)
//						}
//					}
//				}
//			}
//
//			for _, chk := range stmt.Schema.ParseCheckConstraints() {
//				createTableSQL += "CONSTRAINT ? CHECK (?),"
//				values = append(values, clause.Column{Name: chk.Name}, clause.Expr{SQL: chk.Constraint})
//			}
//
//			createTableSQL = strings.TrimSuffix(createTableSQL, ",")
//
//			createTableSQL += ")"
//
//			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
//				createTableSQL += fmt.Sprint(tableOption)
//			}
//
//			errr = tx.Exec(createTableSQL, values...).Error
//			return errr
//		}); err != nil {
//			return err
//		}
//	}
//	return nil
//}

func (m Migrator) CreateTable(values ...interface{}) (err error) {
	if err = m.Migrator.CreateTable(values...); err != nil {
		return
	}
	for _, value := range m.ReorderModels(values, false) {
		if err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
			for _, field := range stmt.Schema.FieldsByDBName {
				if field.Comment != "" {
					if err := m.DB.Exec(
						"COMMENT ON COLUMN ?.? IS ?",
						m.CurrentTable(stmt), clause.Column{Name: field.DBName}, gorm.Expr(m.Migrator.Dialector.Explain("$1", field.Comment)),
					).Error; err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return
		}
	}
	return
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int64

	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw("SELECT * FROM USER_TABLES where table_name = ?", stmt.Table).Row().Scan(&count)
	})

	return count > 0
}

func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	tx := m.DB.Session(&gorm.Session{})
	tx.Exec("SET FOREIGN_KEY_CHECKS = 0;")
	for i := len(values) - 1; i >= 0; i-- {
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", clause.Table{Name: stmt.Table}).Error
		}); err != nil {
			return err
		}
	}
	tx.Exec("SET FOREIGN_KEY_CHECKS = 1;")
	return nil
}

func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM USER_CONSTRAINTS WHERE TABLE_NAME = ? AND CONSTRAINT_NAME = ?", stmt.Table, name,
		).Row().Scan(&count)
	}) == nil && count > 0
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		for _, chk := range stmt.Schema.ParseCheckConstraints() {
			if chk.Name == name {
				return m.DB.Exec(
					"ALTER TABLE ? DROP CHECK ?",
					clause.Table{Name: stmt.Table}, clause.Column{Name: name},
				).Error
			}
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP CONSTRAINT ?",
			clause.Table{Name: stmt.Table}, clause.Column{Name: name},
		).Error
	})
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}, clause.Table{Name: stmt.Table}).Error
	})
}

func (m Migrator) HasIndex(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Raw(
			"SELECT COUNT(*) FROM USER_INDEXES WHERE TABLE_NAME = ? AND INDEX_NAME = ?",
			m.Migrator.DB.NamingStrategy.TableName(stmt.Table),
			m.Migrator.DB.NamingStrategy.IndexName(stmt.Table, name),
		).Row().Scan(&count)
	})

	return count > 0
}
