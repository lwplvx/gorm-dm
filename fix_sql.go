package gormdm

import (
	"regexp"
	"strings"

	"gorm.io/gorm"
)

// 全局预编译正则
var (
	// 1. 必须包含 小写 **excluded** 才处理
	hasExcluded = regexp.MustCompile(`\bexcluded\b`)

	// 2. 必须包含 SET 关键字
	hasSet = regexp.MustCompile(`\bSET\b`)

	// ---------------------- 固定精准匹配 ----------------------
	// 匹配：."version" = version + 1
	regVersion = regexp.MustCompile(`(\."version"\s*=\s*)version\s*\+\s*1`)

	// 匹配：."latest_version" = latest_version + 1
	regLatestVersion = regexp.MustCompile(`(\."latest_version"\s*=\s*)latest_version\s*\+\s*1`)
)

// processFixedVersionSql 严格固定匹配：
// 只处理 ."version" / ."latest_version" 赋值 +1
// 必须满足：包含小写 excluded + 包含 SET
func processFixedVersionSql(sql string) string {
	sql = strings.TrimSpace(sql)

	// 安全校验：两个条件必须同时满足
	if !hasExcluded.MatchString(sql) || !hasSet.MatchString(sql) {
		return sql
	}

	// 打印原 SQL 语句
	// fmt.Println("原 SQL :", sql)
	// 替换 1：."version" = version + 1 → ."version" = "excluded"."version" + 1
	sql = regVersion.ReplaceAllString(sql, `${1}"excluded"."version" + 1`)

	// 替换 2：."latest_version" = latest_version + 1 → ."latest_version" = "excluded"."latest_version"+ 1
	sql = regLatestVersion.ReplaceAllString(sql, `${1}"excluded"."latest_version" + 1`)

	// fmt.Println("新的 SQL :", sql)

	return sql
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
	// fmt.Println("原 SQL 语句:", sql)

	// 转换 MySQL JSON 语法为 DM 语法
	newSQL := processFixedVersionSql(sql)

	// 如果 SQL 发生了变化，更新它
	if newSQL != sql {
		tx.Statement.SQL.Reset()
		tx.Statement.SQL.WriteString(newSQL)
	}

	// 打印转换后的 SQL 语句
	// fmt.Println("转换后 SQL 语句:", newSQL)

}

// 把 MySQL JSON 查询语句 转换成 达梦 DM 语句
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
