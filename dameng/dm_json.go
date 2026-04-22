package dameng

import (
	"reflect"
	"regexp"
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

// // 递归构建表达式
// func buildExpression(builder clause.Builder, expr clause.Expression, isTopLevel bool) {

// 	// 处理 JSONArrayExpression
// 	if jsonArrExpr, ok := expr.(*datatypes.JSONArrayExpression); ok {
// 		BuildJSONArrayExpression(builder, jsonArrExpr)
// 	} else if andConditions, ok := expr.(clause.AndConditions); ok {
// 		// 处理 AND 条件
// 		if isTopLevel {
// 			builder.WriteString(" ( ")
// 		}

// 		for _j, andExpr := range andConditions.Exprs {
// 			if _j > 0 {
// 				// 检查是否为 AND 条件
// 				if _, ok := andExpr.(clause.AndConditions); ok {
// 					builder.WriteString(" AND ")
// 				} else if _, ok := andExpr.(clause.OrConditions); ok {
// 					builder.WriteString(" OR ")
// 				} else {
// 					builder.WriteString(" AND ")
// 				}
// 			}

// 			buildExpression(builder, andExpr, false)
// 		}
// 		if isTopLevel {
// 			builder.WriteString(" ) ")
// 		}

// 	} else if orConditions, ok := expr.(clause.OrConditions); ok {
// 		// 处理 OR 条件
// 		for _, orExpr := range orConditions.Exprs {
// 			buildExpression(builder, orExpr, false)
// 		}

// 	} else {
// 		// 其他表达式使用默认构建
// 		expr.Build(builder)
// 	}
// }

// // 使用反射检查表达式是否包含原始 SQL
// func checkJSONFunctions(expr clause.Expression) bool {
// 	// 检查表达式类型
// 	switch e := expr.(type) {
// 	case clause.Expr:
// 		// 检查原始 SQL 是否包含 JSON 函数
// 		if strings.Contains(e.SQL, "JSON_CONTAINS") && strings.Contains(e.SQL, "JSON_OBJECT") {
// 			return true
// 		}
// 	case clause.AndConditions:
// 		for _, andExpr := range e.Exprs {
// 			if checkJSONFunctions(andExpr) {
// 				return true
// 			}
// 		}
// 	case clause.OrConditions:
// 		for _, orExpr := range e.Exprs {
// 			if checkJSONFunctions(orExpr) {
// 				return true
// 			}
// 		}
// 	}
// 	return false
// }

// 预编译正则表达式，性能更高
var mysqlJSONContainsRegex = regexp.MustCompile(
	`JSON_CONTAINS\s*\(\s*([^,]+?)\s*,\s*JSON_OBJECT\s*\(\s*(.+?)\s*\)\s*\)`,
)

// 将 MySQL 的 JSON_CONTAINS + JSON_OBJECT 语法 转换为 达梦数据库兼容语法
// 例如：
// 输入: JSON_CONTAINS(col, JSON_OBJECT('Label', '50000000'))
// 输出: JSON_CONTAINS(col, '{"Label":"50000000"}')
func ConvertJSON_OBJECTToDamengSql(sql string) string {
	if sql == "" {
		return sql
	}
	// 替换所有匹配的 JSON_CONTAINS 表达式
	return mysqlJSONContainsRegex.ReplaceAllStringFunc(sql, func(matchStr string) string {
		// 提取分组：分组1=字段名，分组2=JSON_OBJECT内部的 key,value,key,value...
		parts := mysqlJSONContainsRegex.FindStringSubmatch(matchStr)
		if len(parts) < 3 {
			return matchStr // 匹配失败，原样返回
		}

		field := strings.TrimSpace(parts[1])
		paramsStr := strings.TrimSpace(parts[2])

		// 按逗号分割参数
		params := strings.Split(paramsStr, ",")
		// 去除每个参数前后空格和引号
		for i := range params {
			params[i] = strings.TrimSpace(params[i])
			params[i] = strings.Trim(params[i], `'"`) // 去掉单/双引号
		}

		// 组装成 {"key":"value", ...}
		var jsonKV []string
		for i := 0; i < len(params); i += 2 {
			if i+1 >= len(params) {
				break
			}
			key := params[i]
			val := params[i+1]
			jsonKV = append(jsonKV, `"`+key+`":"`+val+`"`)
		}
		jsonStr := "{" + strings.Join(jsonKV, ",") + "}"

		// 返回达梦格式
		return `JSON_CONTAINS(` + field + `, '` + jsonStr + `')`
	})
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
	convertedSQL = ConvertJSON_OBJECTToDamengSql(convertedSQL)

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
