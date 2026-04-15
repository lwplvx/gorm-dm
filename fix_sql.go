package gormdm

import (
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
)

func modify_query_sql(tx *gorm.DB) {

	if tx.Statement == nil {
		return
	}

	// 获取当前构建好的 SQL 语句
	sql := tx.Statement.SQL.String()
	if sql == "" {
		return
	}

	// 打印原 SQL 语句
	fmt.Println("原 SQL 语句:", sql)

	// 转换 MySQL JSON 语法为 DM 语法
	newSQL := processAll(sql)
	// 如果 SQL 发生了变化，更新它
	if newSQL != sql {
		tx.Statement.SQL.Reset()
		tx.Statement.SQL.WriteString(newSQL)
	}

	// 打印转换后的 SQL 语句
	fmt.Println("转换后 SQL 语句:", newSQL)

}

func convertToFixSql(tx *gorm.DB) {

	if tx.Statement == nil {
		return
	}

	// 获取当前构建好的 SQL 语句
	sql := tx.Statement.SQL.String()
	if sql == "" {
		return
	}

	// 打印原 SQL 语句
	fmt.Println("原 SQL 语句:", sql)

	// 转换 MySQL JSON 语法为 DM 语法
	newSQL := processAll(sql)
	// 如果 SQL 发生了变化，更新它
	if newSQL != sql {
		tx.Statement.SQL.Reset()
		tx.Statement.SQL.WriteString(newSQL)
	}

	// 打印转换后的 SQL 语句
	fmt.Println("转换后 SQL 语句:", newSQL)

}

// 统一处理所有兼容
func processAll(sql string) string {

	// 转换 MySQL JSON 语法为 DM 语法
	// sql = ConvertMySQLJSONToDM(sql)

	// assignment.Value.Vars[0].SQL
	// "excluded_xxx.version"
	// assignment.Value.SQL
	// "? + 1"
	// assignment.Value.
	// 根据配置的map,硬替换表达式，将表达式中的语句 version 替换为 excluded.version

	// 使用正则表达式替换 version + 1 为 "excluded"."version" + 1
	reVersion := regexp.MustCompile(`(\."version"\s*=\s*)version\s*\+\s*1`)
	sql = reVersion.ReplaceAllString(sql, `${1}"excluded"."version" + 1`)

	reLatestVersion := regexp.MustCompile(`(\."latest_version"\s*=\s*)latest_version\s*\+\s*1`)
	sql = reLatestVersion.ReplaceAllString(sql, `${1}"excluded"."latest_version" + 1`)

	// 2. 替换 GROUP BY ts 为 GROUP BY FLOOR(time / 3600)*3600
	sql = strings.ReplaceAll(sql, "GROUP BY \"ts\" ", "GROUP BY FLOOR(time / 3600)*3600 ")

	// 1. 布尔值
	// sql = convertBoolToDM0or1(sql)

	// 2. LIMIT n → TOP n，支持 LIMIT n OFFSET m
	// sql = convertLimitToTop(sql)

	// 5. 时间格式 2025-01-01T12:00:00+08:00 → 2025-01-01 12:00:00
	// sql = fixDateTimeFormat(sql)

	return sql
}

// ConvertMySQLJSONToDM,  把 MySQL JSON 查询语句 转换成 达梦 DM 语句
// 入参：mysqlWhere 原始 MySQL where 条件
// 返回：dmWhere 转换后的达梦语句
func ConvertMySQLJSONToDM(mysqlSQL string) string {
	// ------------------------------------------------------
	// 规则1：转换 JSON_CONTAINS(COALESCE(col, '[]'), '["value"]')
	// 适用：san、dns、域名数组包含判断
	// 支持带引号的列名和具体的 JSON 值
	// ------------------------------------------------------
	re1 := regexp.MustCompile(`JSON_CONTAINS\(COALESCE\(([\w\"]+), '\[\]'\), '\["([^"]+)"\]'\)`)
	dmSQL := re1.ReplaceAllStringFunc(mysqlSQL, func(s string) string {
		match := re1.FindStringSubmatch(s)
		col := match[1]
		value := match[2]
		// DM 数组包含等价写法
		return "INSTR(" + col + ", '\"" + value + "\"') > 0"
	})

	// ------------------------------------------------------
	// 规则2：转换 JSON_CONTAINS(col, JSON_OBJECT('k1', ?, 'k2', ?))
	// 适用：attributes 多键值匹配
	// ------------------------------------------------------
	re2 := regexp.MustCompile(`JSON_CONTAINS\(([\w\"]+), JSON_OBJECT\('([^']+)', \?, '([^']+)', \?\)\)`)
	dmSQL = re2.ReplaceAllStringFunc(dmSQL, func(s string) string {
		match := re2.FindStringSubmatch(s)
		col := match[1]
		k1 := match[2]
		k2 := match[3]
		// DM 标准 JSON_VALUE 写法
		return "JSON_VALUE(" + col + ", '$." + k1 + "') = ? AND JSON_VALUE(" + col + ", '$." + k2 + "') = ?"
	})

	// ------------------------------------------------------
	// 规则3：转换 JSON_CONTAINS(col, '["value"]')
	// 适用：简单的数组包含判断
	// ------------------------------------------------------
	re3 := regexp.MustCompile(`JSON_CONTAINS\(([\w\"]+), '\["([^"]+)"\]'\)`)
	dmSQL = re3.ReplaceAllStringFunc(dmSQL, func(s string) string {
		match := re3.FindStringSubmatch(s)
		col := match[1]
		value := match[2]
		// DM 数组包含等价写法
		return "INSTR(" + col + ", '\"" + value + "\"') > 0"
	})

	return dmSQL
}

// 全局注册：真正能修改 SQL 的终极钩子
func InterceptSQL_toFix(db *gorm.DB) {
	// =========================================
	// 🔥 必须用 Replace 覆盖 gorm:query
	// 这是唯一能修改 SQL 的方式
	// =========================================
	// originalCallback := db.Callback().Query().Get("gorm:query")

	// db.Callback().Query().Replace("gorm:query", func(tx *gorm.DB) {
	// 	// --------------------------
	// 	// 这里能拿到 最终完整 SQL
	// 	// --------------------------
	// 	sql := tx.Statement.SQL.String()

	// 	// ======================
	// 	// 你想怎么改就怎么改
	// 	// ======================
	// 	if sql != "" {
	// 		// 你的达梦修复
	// 		newSQL := strings.ReplaceAll(sql,
	// 			"GROUP BY \"ts\" ",
	// 			"GROUP BY FLOOR(time / 3600) * 3600",
	// 		)

	// 		// 塞回去（真正生效）
	// 		tx.Statement.SQL.Reset()
	// 		tx.Statement.SQL.WriteString(newSQL)
	// 	}

	// 	// 执行原来的逻辑
	// 	originalCallback(tx)
	// })
}

// // 自定义 GROUP BY 构建器
// func ReplaceGroupBy(db *gorm.DB) {
// 	// 覆盖 GORM 原生的 Group By 构建逻辑
// 	db.Callback().Query().Replace("gorm:query", func(tx *gorm.DB) {
// 		// 重点：如果 Group By 是 "ts"，自动替换成真实表达式
// 		if len(tx.Statement.GroupByExprs) > 0 {
// 			for i, expr := range tx.Statement.GroupByExprs {
// 				sql := expr.SQL(nil)
// 				// 匹配到 Group By ts 就替换
// 				if strings.TrimSpace(sql) == "ts" {
// 					tx.Statement.GroupByExprs[i] = clause.Expr{
// 						SQL: "FLOOR(time / 3600) * 3600",
// 					}
// 				}
// 			}
// 		}

// 		// 执行原生查询
// 		tx.Execute()
// 	})
// }

// // 注册全局钩子：自动把 Group("ts") 替换成 FLOOR(time/3600)*3600
// func registerReplaceGroupTs(db *gorm.DB) {
// 	// 替换 gorm:query 主回调，在生成SQL前修改
// 	db.Callback().Query().Replace("gorm:query", func(tx *gorm.DB) {
// 		// 1. 取出 GROUP BY 子句
// 		if v, ok := tx.Statement.Clauses["GROUP BY"].Expression.(clause.GroupBy); ok {
// 			newColumns := make([]clause.Column, 0, len(v.Columns))
// 			// 2. 遍历每个分组列，替换 "ts"
// 			for _, col := range v.Columns {
// 				if strings.TrimSpace(col.Name) == "ts" {
// 					// 替换为完整表达式，Raw=true 不自动加引号
// 					newColumns = append(newColumns, clause.Column{
// 						Name: "FLOOR(time / 3600) * 3600",
// 						Raw:  true, // 关键：不转义、不加反引号
// 					})
// 				} else {
// 					newColumns = append(newColumns, col)
// 				}
// 			}
// 			// 3. 覆盖回 Statement，替换生效
// 			v.Columns = newColumns
// 			tx.Statement.Clauses["GROUP BY"].Expression = v
// 		}

// 		// 执行原生查询逻辑
// 		tx.Execute()
// 	})
// }
