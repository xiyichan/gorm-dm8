package gorm_dm8

import (
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

func (m Migrator) GetTables() (tableList []string, err error) {
	return tableList, m.DB.Raw("SELECT TABLE_NAME FROM USER_TABLES").Scan(&tableList).Error
}
