package panwei

import (
	"reflect"
	"unsafe"

	"gorm.io/datatypes"
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
				return
			}
			// 默认处理
			c.Build(builder)

		},
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
