package panwei

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

// 替换 mysql 写法到盘维写法
func replaceMysqlSqlToPanWeiSql(builder clause.Builder) {
	// 检查是否是 gorm.Statement
	stmt, ok := builder.(*gorm.Statement)
	if !ok {
		return
	}

	// 获取原始 SQL
	originalSQL := stmt.SQL.String()

	// 转换 JSON   JSON_VALUE 写法转换为正确的写法
	convertedSQL := ConvertBINARYToPanWeiSql(originalSQL)

	// 转换 JSON   JSON_VALUE 写法转换为正确的写法
	convertedSQL = ConvertJSONValueToPanWei(convertedSQL)

	// 如果转换后的 SQL 不同，更新它
	if convertedSQL != originalSQL {
		stmt.SQL.Reset()
		stmt.SQL.WriteString(convertedSQL)
	}
}

// ConvertBINARYToPanWeiSql
// MySQL: WHERE BINARY column = 'value'
// 转为 盘维/PG: WHERE column = 'value' COLLATE "C"
func ConvertBINARYToPanWeiSql(originalSQL string) string {
	// 正则规则：匹配 BINARY + 空格 + 字段名 +  =/LIKE + 值
	// 处理带引号的值和不带引号的值
	// 捕获分组：$1 = 字段名  $2 = 比较运算符  $3 = 值部分
	reg := regexp.MustCompile(`\bBINARY\s+([\w\.]+)\s*(=|LIKE)\s*('.*?'|[^\s;]+)`)

	// 替换成：字段 比较运算符 值 COLLATE "C"
	convertedSQL := reg.ReplaceAllString(originalSQL, `$1 $2 $3 COLLATE "C"`)

	return convertedSQL
}

// ConvertJSONValueToPanWei 自动将 MySQL JSON_VALUE 转换为 盘维 JSON_CONTAINS(JSON_OBJECT)
// 支持：AND / OR 条件
func ConvertJSONValueToPanWei(sql string) string {
	// 匹配单个 JSON_VALUE 条件：JSON_VALUE(col, '$.key') = 'value' 或 JSON_VALUE(col, '$.key') = $1
	jsonValueRegex := regexp.MustCompile(
		`JSON_VALUE\s*\(\s*([^,]+?)\s*,(\s*'\$\.([^']+)')\s*\)\s*=\s*('([^']*)'|\$\d+)`,
	)

	// 先找出所有WHERE子句
	whereRegex := regexp.MustCompile(`(WHERE\s+)(.*)`)
	whereMatch := whereRegex.FindStringSubmatch(sql)
	if len(whereMatch) < 3 {
		return sql // 没有WHERE子句，直接返回
	}

	originalWhere := whereMatch[2]
	wherePrefix := whereMatch[1]

	// 如果没有JSON条件，直接返回
	if !jsonValueRegex.MatchString(originalWhere) {
		return sql
	}

	// 按OR分割条件组，OR的优先级低于AND
	orGroups := regexp.MustCompile(`\s+(OR|or)\s+`).Split(originalWhere, -1)

	var processedGroups []string
	for _, group := range orGroups {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}

		// 按AND分割当前组的条件，AND的优先级高于OR
		// 这里要保留AND的原始大小写
		andConditions := regexp.MustCompile(`\s+(AND|and)\s+`).Split(group, -1)
		andOps := regexp.MustCompile(`\s+(AND|and)\s+`).FindAllString(group, -1)

		// 收集当前组中的条件及其类型
		type condition struct {
			isJSON   bool
			col      string
			key      string
			val      string
			original string
		}

		var conditions []condition

		for _, condStr := range andConditions {
			condStr = strings.TrimSpace(condStr)
			if condStr == "" {
				continue
			}

			if match := jsonValueRegex.FindStringSubmatch(condStr); match != nil {
				col := strings.TrimSpace(match[1])
				key := match[3] // 提取键名，不包含$和引号
				val := match[4] // 提取值（可能是带引号的字符串或参数占位符）

				conditions = append(conditions, condition{
					isJSON:   true,
					col:      col,
					key:      key,
					val:      val,
					original: condStr,
				})
			} else {
				conditions = append(conditions, condition{
					isJSON:   false,
					original: condStr,
				})
			}
		}

		// 处理当前OR组内的条件，将连续的JSON条件按列合并
		var processedConditions []string
		currentJSONGroup := make(map[string][]map[string]string) // col -> []{key, val}
		currentNonJSON := ""

		for j, cond := range conditions {
			if cond.isJSON {
				// JSON条件，添加到当前组
				currentJSONGroup[cond.col] = append(currentJSONGroup[cond.col], map[string]string{
					"key": cond.key,
					"val": cond.val,
				})
			} else {
				// 非JSON条件，先处理之前的JSON组
				if len(currentJSONGroup) > 0 {
					var jsonParts []string
					for col, kvPairs := range currentJSONGroup {
						var kv []string
						for _, kvp := range kvPairs {
							key := `'` + kvp["key"] + `'`
							val := kvp["val"]
							// 检查值是否是参数占位符（以$开头）
							if !strings.HasPrefix(val, "$") {
								// 如果是字符串值，检查是否已经包含单引号
								if !strings.HasPrefix(val, "'") {
									val = `'` + val + `'`
								}
							}
							kv = append(kv, key, val)
						}
						jsonObj := "JSON_OBJECT(" + strings.Join(kv, ", ") + ")"
						jsonContains := "JSON_CONTAINS(" + col + ", " + jsonObj + ")"
						jsonParts = append(jsonParts, jsonContains)
					}
					if currentNonJSON != "" {
						processedConditions = append(processedConditions, currentNonJSON+
							strings.Join(jsonParts, " AND "))
					} else {
						processedConditions = append(processedConditions, strings.Join(jsonParts, " AND "))
					}
					currentJSONGroup = make(map[string][]map[string]string)
					currentNonJSON = ""
				}

				// 添加非JSON条件
				if currentNonJSON != "" {
					if j > 0 && len(andOps) >= j {
						currentNonJSON += andOps[j-1]
					}
					currentNonJSON += cond.original
				} else {
					currentNonJSON = cond.original
				}
			}

			// 如果是最后一个条件，处理剩余的JSON组
			if j == len(conditions)-1 && len(currentJSONGroup) > 0 {
				var jsonParts []string
				for col, kvPairs := range currentJSONGroup {
					var kv []string
					for _, kvp := range kvPairs {
						key := `'` + kvp["key"] + `'`
						val := kvp["val"]
						// 检查值是否是参数占位符（以$开头）
						if !strings.HasPrefix(val, "$") {
							// 如果是字符串值，检查是否已经包含单引号
							if !strings.HasPrefix(val, "'") {
								val = `'` + val + `'`
							}
						}
						kv = append(kv, key, val)
					}
					jsonObj := "JSON_OBJECT(" + strings.Join(kv, ", ") + ")"
					jsonContains := "JSON_CONTAINS(" + col + ", " + jsonObj + ")"
					jsonParts = append(jsonParts, jsonContains)
				}

				if currentNonJSON != "" {
					if j > 0 && len(andOps) >= j {
						currentNonJSON += andOps[j-1]
					}
					processedConditions = append(processedConditions, currentNonJSON+
						strings.Join(jsonParts, " AND "))
				} else {
					processedConditions = append(processedConditions, strings.Join(jsonParts, " AND "))
				}
			}
		}

		// 用原始的AND运算符连接当前OR组内的处理结果
		if len(processedConditions) > 0 {
			// 查找当前组中第一个AND运算符的大小写
			andOp := " AND "
			if len(andOps) > 0 {
				andOp = andOps[0] // 使用第一个AND运算符的原始大小写
			}
			processedGroups = append(processedGroups, strings.Join(processedConditions, andOp))
		} else {
			processedGroups = append(processedGroups, group)
		}
	}

	// 用大写OR运算符连接所有OR组
	newWhereClause := ""
	for i, group := range processedGroups {
		if i > 0 {
			newWhereClause += " OR "
		}
		newWhereClause += group
	}

	// 替换原SQL中的WHERE子句
	return strings.Replace(sql, wherePrefix+originalWhere, wherePrefix+newWhereClause, 1)
}

// // ConvertJSONValueToPanWei
// func ConvertJSONValueToPanWei(originalSQL string) string {
// 	// 1. 先处理所有 JSON_VALUE 条件，提取：列、key、值
// 	// 匹配模式: JSON_VALUE(col, '$.key') = 'value'
// 	re := regexp.MustCompile(`JSON_VALUE\s*\(\s*([^,]+?)\s*,\s*'\$\.([^']+)'\s*\)\s*=\s*'([^']*)'`)

// 	// 存储提取出来的 key/value
// 	var colName string
// 	var kvPairs []string

// 	// 先收集所有 key-value
// 	re.ReplaceAllStringFunc(originalSQL, func(m string) string {
// 		match := re.FindStringSubmatch(m)
// 		if len(match) >= 4 {
// 			colName = strings.TrimSpace(match[1])
// 			key := match[2]
// 			val := match[3]
// 			kvPairs = append(kvPairs, `'`+key+`'`, `'`+val+`'`)
// 		}
// 		return m
// 	})

// 	// 如果没有匹配到 JSON_VALUE，直接返回原SQL
// 	if colName == "" || len(kvPairs) == 0 {
// 		return originalSQL
// 	}

// 	// 2. 构建 JSON_CONTAINS 语句
// 	jsonObj := "JSON_OBJECT(" + strings.Join(kvPairs, ", ") + ")"
// 	replaceStr := "JSON_CONTAINS(" + colName + ", " + jsonObj + ")"

// 	// 3. 替换整个 WHERE 条件
// 	// 匹配：JSON_VALUE(...) AND JSON_VALUE(...)
// 	fullRe := regexp.MustCompile(`JSON_VALUE\(.+?\)\s*=\s*'[^']*'\s*(?:AND\s*JSON_VALUE\(.+?\)\s*=\s*'[^']*')*`)
// 	return fullRe.ReplaceAllString(originalSQL, replaceStr)
// }
