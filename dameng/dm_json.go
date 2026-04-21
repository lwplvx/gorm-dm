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

// jsonQueryJoin 将 keys 转换为 JSON 查询路径
func jsonQueryJoin(keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	return "." + strings.Join(keys, ".")
}

// BuildJSONArrayExpression 构建 JSONArrayExpression SQL

// BuildJSONArrayExpression 构建 JSONArrayExpression SQL
func BuildJSONArrayExpression(builder clause.Builder, jsonExpr *datatypes.JSONArrayExpression) {
	stmt, ok := builder.(*gorm.Statement)
	if !ok {
		return
	}

	// 获取字段值
	contains := getJSONArrayExpressionField(jsonExpr, "contains").(bool)
	in := getJSONArrayExpressionField(jsonExpr, "in").(bool)
	column := getJSONArrayExpressionField(jsonExpr, "column").(string)
	keys := getJSONArrayExpressionField(jsonExpr, "keys").([]string)
	equalsValue := getJSONArrayExpressionField(jsonExpr, "equalsValue")

	// 构建 SQL
	if contains {
		// 构建 JSON_CONTAINS(column, '["value"]') = 1 格式
		builder.WriteString("JSON_CONTAINS(")
		builder.WriteString(stmt.Quote(column))
		builder.WriteString(", '")

		// 写入值，确保是 JSON 字符串格式
		if strVal, ok := equalsValue.(string); ok {
			// 为字符串值构建 JSON 数组
			builder.WriteString("[\"")
			builder.WriteString(strVal)
			builder.WriteString("\"]")
		} else {
			// 其他类型直接转换为字符串
			builder.WriteString("[")
			builder.AddVar(stmt, equalsValue)
			builder.WriteString("]")
		}

		builder.WriteString("'")

		// 添加路径
		if len(keys) > 0 {
			builder.WriteByte(',')
			builder.WriteString("'$")
			builder.WriteString(jsonQueryJoin(keys))
			builder.WriteString("'")
		}

		builder.WriteString(") = 1")
	} else if in {
		// 构建 JSON_CONTAINS('["value1","value2"]', column) = 1 格式
		builder.WriteString("JSON_CONTAINS('[")

		// 处理数组值
		if arrayValues, ok := equalsValue.([]interface{}); ok {
			for i, val := range arrayValues {
				if i > 0 {
					builder.WriteString(",")
				}
				if strVal, ok := val.(string); ok {
					builder.WriteString("\"")
					builder.WriteString(strVal)
					builder.WriteString("\"")
				} else {
					builder.AddVar(stmt, val)
				}
			}
		} else {
			// 如果不是数组，直接作为单个值处理
			if strVal, ok := equalsValue.(string); ok {
				builder.WriteString("\"")
				builder.WriteString(strVal)
				builder.WriteString("\"")
			} else {
				builder.AddVar(stmt, equalsValue)
			}
		}

		builder.WriteString("]'")
		builder.WriteByte(',')

		// 添加路径
		if len(keys) > 0 {
			builder.WriteString("JSON_EXTRACT(")
		}

		builder.WriteString(stmt.Quote(column))

		// 结束路径
		if len(keys) > 0 {
			builder.WriteByte(',')
			builder.WriteString("'$")
			builder.WriteString(jsonQueryJoin(keys))
			builder.WriteString("'")
			builder.WriteByte(')')
		}

		builder.WriteString(") = 1")
	}
}

// GetJSONClauseBuilders 获取 JSON 相关的子句构建器
func GetJSONClauseBuilders() map[string]func(clause.Clause, clause.Builder) {
	return map[string]func(clause.Clause, clause.Builder){
		"WHERE": func(c clause.Clause, builder clause.Builder) {
			if values, ok := c.Expression.(clause.Where); ok && len(values.Exprs) > 0 {
				// 检查是否有 JSON_CONTAINS 和 JSON_OBJECT, 有的话替换为达梦的语法
				hasJSONFunctions := false
				for _, expr := range values.Exprs {
					if checkJSONFunctions(expr) {
						hasJSONFunctions = true
						break
					}
				}

				// 如果包含 JSON 函数，先构建原始 SQL 再转换
				if hasJSONFunctions {
					// 检查是否是 gorm.Statement
					stmt, ok := builder.(*gorm.Statement)
					if !ok {
						c.Build(builder)
						return
					}
					// 构建原始 SQL
					c.Build(builder)
					// 获取原始 SQL
					originalSQL := stmt.SQL.String()
					// 转换 JSON 函数
					convertedSQL := ConvertJSON_OBJECTToDamengSql(originalSQL)
					// 如果转换后的 SQL 不同，更新它
					if convertedSQL != originalSQL {
						stmt.SQL.Reset()
						stmt.SQL.WriteString(convertedSQL)
					}
					return
				}

				hasJSONArray := false

				// 递归检查是否有 JSONArrayExpression，支持嵌套的 AND/OR 条件
				var checkJSONArray func(expr clause.Expression) bool
				checkJSONArray = func(expr clause.Expression) bool {
					if _, ok := expr.(*datatypes.JSONArrayExpression); ok {
						return true
					}
					if andConditions, ok := expr.(clause.AndConditions); ok {
						for _, andExpr := range andConditions.Exprs {
							if checkJSONArray(andExpr) {
								return true
							}
						}
					}
					if orConditions, ok := expr.(clause.OrConditions); ok {
						for _, orExpr := range orConditions.Exprs {
							if checkJSONArray(orExpr) {
								return true
							}
						}
					}
					return false
				}

				// 检查所有表达式
				for _, expr := range values.Exprs {
					if checkJSONArray(expr) {
						hasJSONArray = true
						break
					}
				}

				if hasJSONArray {
					builder.WriteString(" WHERE ")

					// 构建顶层表达式
					for _, expr := range values.Exprs {

						// 检查是否为 AND 条件
						if _, ok := expr.(clause.AndConditions); ok {
							builder.WriteString(" AND ")
						}
						// 检查是否为 OR 条件
						if _, ok := expr.(clause.OrConditions); ok {
							builder.WriteString(" OR ")
						}

						buildExpression(builder, expr, true)
					}
					return
				}
			}
			// 默认处理
			c.Build(builder)

			// mysql to dameng sql 替换
			replaceMysqlSqlToDMSql(builder)
		},
	}
}

// 递归构建表达式
func buildExpression(builder clause.Builder, expr clause.Expression, isTopLevel bool) {

	// 处理 JSONArrayExpression
	if jsonArrExpr, ok := expr.(*datatypes.JSONArrayExpression); ok {
		BuildJSONArrayExpression(builder, jsonArrExpr)
	} else if andConditions, ok := expr.(clause.AndConditions); ok {
		// 处理 AND 条件
		if isTopLevel {
			builder.WriteString(" ( ")
		}

		for _j, andExpr := range andConditions.Exprs {
			if _j > 0 {
				// 检查是否为 AND 条件
				if _, ok := andExpr.(clause.AndConditions); ok {
					builder.WriteString(" AND ")
				} else if _, ok := andExpr.(clause.OrConditions); ok {
					builder.WriteString(" OR ")
				} else {
					builder.WriteString(" AND ")
				}
			}

			buildExpression(builder, andExpr, false)
		}
		if isTopLevel {
			builder.WriteString(" ) ")
		}

	} else if orConditions, ok := expr.(clause.OrConditions); ok {
		// 处理 OR 条件
		for _, orExpr := range orConditions.Exprs {
			buildExpression(builder, orExpr, false)
		}

	} else {
		// 其他表达式使用默认构建
		expr.Build(builder)
	}
}

// 使用反射检查表达式是否包含原始 SQL
func checkJSONFunctions(expr clause.Expression) bool {
	// 检查表达式类型
	switch e := expr.(type) {
	case clause.Expr:
		// 检查原始 SQL 是否包含 JSON 函数
		if strings.Contains(e.SQL, "JSON_CONTAINS") && strings.Contains(e.SQL, "JSON_OBJECT") {
			return true
		}
	case clause.AndConditions:
		for _, andExpr := range e.Exprs {
			if checkJSONFunctions(andExpr) {
				return true
			}
		}
	case clause.OrConditions:
		for _, orExpr := range e.Exprs {
			if checkJSONFunctions(orExpr) {
				return true
			}
		}
	}
	return false
}

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

	// 如果转换后的 SQL 不同，更新它
	if convertedSQL != originalSQL {
		stmt.SQL.Reset()
		stmt.SQL.WriteString(convertedSQL)
	}
}
