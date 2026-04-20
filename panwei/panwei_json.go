package panwei

import (
	"reflect"
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
		// builder.WriteString(stmt.Quote(json.column))
		// 		builder.WriteString(" ? ")
		// 		builder.AddVar(stmt, json.equalsValue)

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
