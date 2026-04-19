// 本方言包基于gorm v1.24.2开发，需要配合达梦数据库驱动使用
package gormdm

import (
	"fmt"

	dameng "github.com/lwplvx/gorm-dm/dameng"
	panwei "github.com/lwplvx/gorm-dm/panwei"
	mysql "gorm.io/driver/mysql"

	"gorm.io/gorm" // 引入gorm v2包
)

func OpenDb(dsn string, driverName string) gorm.Dialector {
	switch driverName {
	case "dm":
		return dameng.Open(dsn)
	case "panwei":
		return panwei.Open(dsn)
	case "mysql":
		return mysql.Open(dsn)
	default:

		fmt.Printf("驱动 %s 未集成\n", driverName)
		return nil
	}
}

func Open(dsn string) gorm.Dialector {
	return OpenDb(dsn, "dm")
}
