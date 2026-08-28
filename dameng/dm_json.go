package dameng

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unsafe"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// getJSONArrayExpressionField 获取 JSONArrayExpression 结构体的字段值
func getJSONArrayExpressionField(json *datatypes.JSONArrayExpression, fieldName string) interface{} {
	v := reflect.ValueOf(json).Elem()
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return nil
	}

	if field.CanInterface() {
		return field.Interface()
	}

	fieldAddr := unsafe.Pointer(field.UnsafeAddr())

	switch field.Kind() {
	case reflect.Bool:
		return *(*bool)(fieldAddr)
	case reflect.String:
		return *(*string)(fieldAddr)
	case reflect.Slice:
		if field.Type().Elem().Kind() == reflect.String {
			slicePtr := (*[]string)(fieldAddr)
			result := make([]string, len(*slicePtr))
			copy(result, *slicePtr)
			return result
		}
	case reflect.Interface:
		return *(*interface{})(fieldAddr)
	default:
		return reflect.NewAt(field.Type(), fieldAddr).Elem().Interface()
	}

	return nil
}

// GetJSONClauseBuilders 获取 JSON 相关的子句构建器
func GetJSONClauseBuilders() map[string]func(clause.Clause, clause.Builder) {
	return map[string]func(clause.Clause, clause.Builder){
		"WHERE": func(c clause.Clause, builder clause.Builder) {
			if values, ok := c.Expression.(clause.Where); ok && len(values.Exprs) > 0 {
				// 递归替换所有 datatypes.JSONArrayExpression 为 dameng.JSONArrayExpression
				newExprs := make([]clause.Expression, len(values.Exprs))
				for i, expr := range values.Exprs {
					newExprs[i] = replaceJSONArrayExpressions(expr)
				}

				// 使用替换后的表达式构建 SQL
				newWhere := clause.Where{Exprs: newExprs}

				builder.WriteString(" WHERE ")
				newWhere.Build(builder)

				// 执行 SQL 替换
				replaceMysqlSqlToDMSql(builder)
				return
			}

			// 默认处理
			c.Build(builder)
			// 执行 SQL 替换
			replaceMysqlSqlToDMSql(builder)
		},
		"ORDER BY": func(c clause.Clause, builder clause.Builder) {
			// 默认处理
			c.Build(builder)

			// mysql to dameng sql 替换
			replaceMysqlSqlToDMSql(builder)
		},
	}
}

// 预编译正则表达式，性能更高
var mysqlJSONContainsRegex = regexp.MustCompile(
	`JSON_CONTAINS\s*\(\s*([^,]+?)\s*,\s*JSON_OBJECT\s*\(\s*(.+?)\s*\)\s*\)`,
)

// 匹配 JSON_OBJECT 内的一组 key/value：'key', value，其中 value 可以是字面量 'xxx' 或占位符 ?
var mysqlJSONKvRegex = regexp.MustCompile(`'([^']+)'\s*,\s*('([^']*)'|\?)`)

// questionOrdinalBefore 统计 sql[0:pos] 区间内字符串字面量之外的 ? 占位符个数（含 pos 位置）。
// 达梦方言所有绑定参数都写成 ?，因此第 N 个真实 ? 对应 stmt.Vars[N-1]。
func questionOrdinalBefore(sql string, pos int) int {
	n := 0
	inStr := false
	for i := 0; i <= pos && i < len(sql); i++ {
		switch sql[i] {
		case '\'':
			inStr = !inStr
		case '?':
			if !inStr {
				n++
			}
		}
	}
	return n
}

// removeConsumedVars 从 stmt.Vars 中移除已被内联到 SQL 的占位符对应参数（索引从大到小删，避免索引错位）。
func removeConsumedVars(stmt *gorm.Statement, consumedVars []int) {
	seen := make(map[int]bool)
	var uniqueVars []int
	for _, v := range consumedVars {
		if !seen[v] {
			seen[v] = true
			uniqueVars = append(uniqueVars, v)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(uniqueVars)))
	for _, idx := range uniqueVars {
		if idx >= 0 && idx < len(stmt.Vars) {
			stmt.Vars = append(stmt.Vars[:idx], stmt.Vars[idx+1:]...)
		}
	}
}

// 将 MySQL 的 JSON_CONTAINS + JSON_OBJECT 语法 转换为 达梦数据库兼容语法
// 例如：
// 输入: JSON_CONTAINS(col, JSON_OBJECT('Label', '50000000'))
// 输出: JSON_CONTAINS(col, '{"Label":"50000000"}')
//
// 关键点：JSON_OBJECT 内的值若为占位符 ?，必须把对应参数**内联**进 JSON 字符串，
// 并返回被消费的参数下标（由调用方从 stmt.Vars 移除），否则 ? 会残留在 JSON 字符串
// 字面量内，导致 SQL 真实占位符数量与参数数量不一致（报错 expected N arguments, got M
// 或静默返回错误结果）。
func ConvertJSON_OBJECTToDamengSql(sql string, stmt *gorm.Statement) (string, []int) {
	if sql == "" || stmt == nil {
		return sql, nil
	}

	matches := mysqlJSONContainsRegex.FindAllStringSubmatchIndex(sql, -1)
	if len(matches) == 0 {
		return sql, nil
	}

	var consumedVars []int
	result := sql

	// 从右到左替换，保证字节偏移在替换后仍然有效
	for i := len(matches) - 1; i >= 0; i-- {
		m := matches[i]
		fullStart, fullEnd := m[0], m[1]
		field := strings.TrimSpace(sql[m[2]:m[3]])
		paramsStr := sql[m[4]:m[5]]

		jsonObj := make(map[string]string)
		var matchConsumed []int
		resolvable := true

		for _, km := range mysqlJSONKvRegex.FindAllStringSubmatchIndex(paramsStr, -1) {
			key := paramsStr[km[2]:km[3]]
			valStr := paramsStr[km[4]:km[5]]

			if valStr == "?" {
				// 占位符：确定它在整条 SQL 中是第几个 ?，映射到 stmt.Vars
				globalPos := m[4] + km[4]
				ordinal := questionOrdinalBefore(sql, globalPos)
				varIndex := ordinal - 1
				if varIndex < 0 || varIndex >= len(stmt.Vars) {
					resolvable = false
					break
				}
				jsonObj[key] = fmt.Sprint(stmt.Vars[varIndex])
				matchConsumed = append(matchConsumed, varIndex)
			} else {
				// 字面量：去掉外层引号
				jsonObj[key] = strings.Trim(valStr, "'")
			}
		}

		if !resolvable || len(jsonObj) == 0 {
			continue // 无法解析，保留原样，避免产生更坏的 SQL
		}

		jsonBytes, _ := json.Marshal(jsonObj)
		jsonStr := strings.ReplaceAll(string(jsonBytes), "'", "''")
		replacement := `JSON_CONTAINS(` + field + `, '` + jsonStr + `')`

		result = result[:fullStart] + replacement + result[fullEnd:]
		consumedVars = append(consumedVars, matchConsumed...)
	}

	return result, consumedVars
}

// 预编译正则
var backtickPairRegex = regexp.MustCompile("`([^`]*)`")

// ConvertMySQLQuotesToDamengSafe
// 安全替换：只处理成对出现的 `xxx` → "xxx"
func ConvertMySQLQuotesToDamengSafe(sql string) string {
	return backtickPairRegex.ReplaceAllStringFunc(sql, func(match string) string {
		return `"` + strings.Trim(match, "`") + `"`
	})
}

// 替换 mysql 写法到达梦写法
func replaceMysqlSqlToDMSql(builder clause.Builder) {
	// 检查是否是 gorm.Statement
	stmt, ok := builder.(*gorm.Statement)
	if !ok {
		return
	}

	// 获取原始 SQL
	originalSQL := stmt.SQL.String()
	// 如果语句包含 mysql 的字段单引号 ``，则替换为达梦的双引号 ""，确保引号成对出现
	convertedSQL := ConvertMySQLQuotesToDamengSafe(originalSQL)

	// 转换 JSON 函数
	convertedSQL, consumedVars := ConvertJSON_OBJECTToDamengSql(convertedSQL, stmt)
	// JSON_OBJECT 内被内联的 ? 占位符对应的参数要从 stmt.Vars 中移除，
	// 否则 SQL 占位符数量与参数数量不一致（生产报错 expected 2 arguments, got 3）。
	if len(consumedVars) > 0 {
		removeConsumedVars(stmt, consumedVars)
	}

	// 如果转换后的 SQL 不同，更新它
	if convertedSQL != originalSQL {
		stmt.SQL.Reset()
		stmt.SQL.WriteString(convertedSQL)
	}
}

func getNewJSONArrayExpression(e *datatypes.JSONArrayExpression) *JSONArrayExpression {

	// 获取字段值
	contains := getJSONArrayExpressionField(e, "contains").(bool)
	in := getJSONArrayExpressionField(e, "in").(bool)
	column := getJSONArrayExpressionField(e, "column").(string)
	keys := getJSONArrayExpressionField(e, "keys").([]string)
	equalsValue := getJSONArrayExpressionField(e, "equalsValue")

	// 旧表达式 e 转换成你的自定义 JSONArrayExpression
	return &JSONArrayExpression{
		contains:    contains,
		in:          in,
		column:      column,
		keys:        keys,
		equalsValue: equalsValue,
	}
}

// replaceJSONArrayExpressions 递归替换条件表达式中的 JSONArrayExpression
func replaceJSONArrayExpressions(expr clause.Expression) clause.Expression {
	switch e := expr.(type) {
	case *datatypes.JSONArrayExpression:
		// datatypes.JSONArrayExpression替换为自定义的 dameng.JSONArrayExpression
		return getNewJSONArrayExpression(e)

	case clause.AndConditions:
		// 递归处理 AND 条件
		newExprs := make([]clause.Expression, len(e.Exprs))
		for i, expr := range e.Exprs {
			newExprs[i] = replaceJSONArrayExpressions(expr)
		}
		e.Exprs = newExprs
		return e

	case clause.OrConditions:
		// 递归处理 OR 条件
		newExprs := make([]clause.Expression, len(e.Exprs))
		for i, expr := range e.Exprs {
			newExprs[i] = replaceJSONArrayExpressions(expr)
		}
		e.Exprs = newExprs
		return e

	default:
		// 其他类型表达式保持不变
		return expr
	}
}
