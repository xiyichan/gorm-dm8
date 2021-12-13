package gorm_dm8

import (
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}
