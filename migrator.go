package gorm_dm8

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

//
//func (m Migrator) FullDataTypeOf(field *schema.Field) clause.Expr {
//	expr := m.Migrator.FullDataTypeOf(field)
//
//	if value, ok := field.TagSettings["COMMENT"]; ok {
//		expr.SQL += " COMMENT " + m.Dialector.Explain("?", value)
//	}
//
//	return expr
//}
//
//func (m Migrator) AlterColumn(value interface{}, field string) error {
//	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
//		if field := stmt.Schema.LookUpField(field); field != nil {
//			return m.DB.Exec(
//				"ALTER TABLE ? MODIFY COLUMN ? ?",
//				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, m.FullDataTypeOf(field),
//			).Error
//		}
//		return fmt.Errorf("failed to look up field with name: %s", field)
//	})
//}
//
//func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
//	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
//
//		var field *schema.Field
//		if f := stmt.Schema.LookUpField(oldName); f != nil {
//			oldName = f.DBName
//			field = f
//		}
//
//		if f := stmt.Schema.LookUpField(newName); f != nil {
//			newName = f.DBName
//			field = f
//		}
//
//		if field != nil {
//			return m.DB.Exec(
//				"ALTER TABLE ? CHANGE ? ? ?",
//				clause.Table{Name: stmt.Table}, clause.Column{Name: oldName},
//				clause.Column{Name: newName}, m.FullDataTypeOf(field),
//			).Error
//		}
//
//		return fmt.Errorf("failed to look up field with name: %s", newName)
//	})
//}
//
//
//func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
//
//	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
//		err := m.DropIndex(value, oldName)
//		if err != nil {
//			return err
//		}
//
//		if idx := stmt.Schema.LookIndex(newName); idx == nil {
//			if idx = stmt.Schema.LookIndex(oldName); idx != nil {
//				opts := m.BuildIndexOptions(idx.Fields, stmt)
//				values := []interface{}{clause.Column{Name: newName}, clause.Table{Name: stmt.Table}, opts}
//
//				createIndexSQL := "CREATE "
//				if idx.Class != "" {
//					createIndexSQL += idx.Class + " "
//				}
//				createIndexSQL += "INDEX ? ON ??"
//
//				if idx.Type != "" {
//					createIndexSQL += " USING " + idx.Type
//				}
//
//				return m.DB.Exec(createIndexSQL, values...).Error
//			}
//		}
//
//		return m.CreateIndex(value, newName)
//	})
//
//}
//
//func (m Migrator) DropTable(values ...interface{}) error {
//	values = m.ReorderModels(values, false)
//	tx := m.DB.Session(&gorm.Session{})
//	tx.Exec("SET FOREIGN_KEY_CHECKS = 0;")
//	for i := len(values) - 1; i >= 0; i-- {
//		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
//			return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", clause.Table{Name: stmt.Table}).Error
//		}); err != nil {
//			return err
//		}
//	}
//	tx.Exec("SET FOREIGN_KEY_CHECKS = 1;")
//	return nil
//}
//
//func (m Migrator) DropConstraint(value interface{}, name string) error {
//	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
//		constraint, chk, table := m.GuessConstraintAndTable(stmt, name)
//		if chk != nil {
//			return m.DB.Exec("ALTER TABLE ? DROP CHECK ?", clause.Table{Name: stmt.Table}, clause.Column{Name: chk.Name}).Error
//		}
//		if constraint != nil {
//			name = constraint.Name
//		}
//
//		return m.DB.Exec(
//			"ALTER TABLE ? DROP FOREIGN KEY ?", clause.Table{Name: table}, clause.Column{Name: name},
//		).Error
//	})
//}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		err := m.DropIndex(value, oldName)
		if err != nil {
			return err
		}

		if idx := stmt.Schema.LookIndex(newName); idx == nil {
			if idx = stmt.Schema.LookIndex(oldName); idx != nil {
				opts := m.BuildIndexOptions(idx.Fields, stmt)
				values := []interface{}{clause.Column{Name: newName}, clause.Table{Name: stmt.Table}, opts}

				createIndexSQL := "CREATE "
				if idx.Class != "" {
					createIndexSQL += idx.Class + " "
				}
				createIndexSQL += "INDEX ? ON ??"

				if idx.Type != "" {
					createIndexSQL += " USING " + idx.Type
				}

				return m.DB.Exec(createIndexSQL, values...).Error
			}
		}

		return m.CreateIndex(value, newName)
	})

}
