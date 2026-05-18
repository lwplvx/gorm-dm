package panwei

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unsafe"

	panweidbgo "github.com/lwplvx/gorm-dm/panwei/gaussdb-go"
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

	// 转换 BINARY 写法转换为正确的写法
	convertedSQL := ConvertBINARYToPanWeiSql(originalSQL)

	// 转换 JSON_VALUE 写法
	convertedSQL, consumedVars := ConvertJSONValueToPanWei(convertedSQL, stmt)

	// 转换 JSON_CONTAINS(JSON_OBJECT(...)) 写法
	convertedSQL, consumedVars2 := ConvertJSONContainsToPanWei(convertedSQL, stmt)

	// 转换 JSON_CONTAINS(col, $N) 裸参数写法
	convertedSQL, consumedVars3 := ConvertJSONContainsParam(convertedSQL, stmt)

	// 转换 JSON_CONTAINS(col, JSON_ARRAY($N)) 写法
	convertedSQL, consumedVars4 := ConvertJSONContainsArray(convertedSQL, stmt)

	allConsumedVars := append(consumedVars, consumedVars2...)
	allConsumedVars = append(allConsumedVars, consumedVars3...)
	allConsumedVars = append(allConsumedVars, consumedVars4...)
	if len(allConsumedVars) > 0 {
		removeConsumedVars(stmt, allConsumedVars)
	}

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

// ConvertJSONValueToPanWei 自动将 MySQL JSON_VALUE 转换为 盘维 col::jsonb @> 'json'::jsonb
// 支持：AND / OR 条件
func ConvertJSONValueToPanWei(sql string, stmt *gorm.Statement) (string, []int) {
	jsonValueRegex := regexp.MustCompile(
		`JSON_VALUE\s*\(\s*([^,]+?)\s*,(\s*'\$\.([^']+)')\s*\)\s*=\s*('([^']*)'|\$\d+)`,
	)

	whereRegex := regexp.MustCompile(`(WHERE\s+)(.*)`)
	whereMatch := whereRegex.FindStringSubmatch(sql)
	if len(whereMatch) < 3 {
		return sql, nil
	}

	originalWhere := whereMatch[2]
	wherePrefix := whereMatch[1]

	if !jsonValueRegex.MatchString(originalWhere) {
		return sql, nil
	}

	orGroups := regexp.MustCompile(`\s+(OR|or)\s+`).Split(originalWhere, -1)

	var processedGroups []string
	var allConsumedVars []int

	for _, group := range orGroups {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}

		andConditions := regexp.MustCompile(`\s+(AND|and)\s+`).Split(group, -1)
		andOps := regexp.MustCompile(`\s+(AND|and)\s+`).FindAllString(group, -1)

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
				key := match[3]
				val := match[4]

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

		var processedConditions []string
		currentJSONGroup := make(map[string][]map[string]string)
		currentNonJSON := ""

		for j, cond := range conditions {
			if cond.isJSON {
				currentJSONGroup[cond.col] = append(currentJSONGroup[cond.col], map[string]string{
					"key": cond.key,
					"val": cond.val,
				})
			} else {
				if len(currentJSONGroup) > 0 {
					jsonParts, consumedVars := buildJSONBContainsParts(currentJSONGroup, stmt)
					allConsumedVars = append(allConsumedVars, consumedVars...)
					if currentNonJSON != "" {
						processedConditions = append(processedConditions, currentNonJSON+
							strings.Join(jsonParts, " AND "))
					} else {
						processedConditions = append(processedConditions, strings.Join(jsonParts, " AND "))
					}
					currentJSONGroup = make(map[string][]map[string]string)
					currentNonJSON = ""
				}

				if currentNonJSON != "" {
					if j > 0 && len(andOps) >= j {
						currentNonJSON += andOps[j-1]
					}
					currentNonJSON += cond.original
				} else {
					currentNonJSON = cond.original
				}
			}

			if j == len(conditions)-1 && len(currentJSONGroup) > 0 {
				jsonParts, consumedVars := buildJSONBContainsParts(currentJSONGroup, stmt)
				allConsumedVars = append(allConsumedVars, consumedVars...)

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

		if len(processedConditions) > 0 {
			andOp := " AND "
			if len(andOps) > 0 {
				andOp = andOps[0]
			}
			processedGroups = append(processedGroups, strings.Join(processedConditions, andOp))
		} else {
			processedGroups = append(processedGroups, group)
		}
	}

	newWhereClause := ""
	for i, group := range processedGroups {
		if i > 0 {
			newWhereClause += " OR "
		}
		newWhereClause += group
	}

	return strings.Replace(sql, wherePrefix+originalWhere, wherePrefix+newWhereClause, 1), allConsumedVars
}

func varOffset(stmt *gorm.Statement) int {
	if len(stmt.Vars) > 0 {
		switch stmt.Vars[0].(type) {
		case panweidbgo.QueryExecMode:
			return 1
		}
	}
	return 0
}

func buildJSONBContainsParts(jsonGroup map[string][]map[string]string, stmt *gorm.Statement) ([]string, []int) {
	offset := varOffset(stmt)
	var jsonParts []string
	var consumedVars []int

	for col, kvPairs := range jsonGroup {
		jsonObj := make(map[string]string)
		for _, kvp := range kvPairs {
			key := kvp["key"]
			val := kvp["val"]

			if strings.HasPrefix(val, "$") {
				n, err := strconv.Atoi(val[1:])
				if err == nil {
					varIndex := n - 1 + offset
					if varIndex < len(stmt.Vars) {
						jsonObj[key] = fmt.Sprint(stmt.Vars[varIndex])
						consumedVars = append(consumedVars, varIndex)
					}
				}
			} else {
				jsonObj[key] = strings.Trim(val, "'")
			}
		}

		jsonBytes, _ := json.Marshal(jsonObj)
		jsonStr := strings.ReplaceAll(string(jsonBytes), "'", "''")
		jsonParts = append(jsonParts, "JSON_CONTAINS(NULLIF("+col+", ''), '"+jsonStr+"')")
	}

	return jsonParts, consumedVars
}

func removeConsumedVars(stmt *gorm.Statement, consumedVars []int) {
	seen := make(map[int]bool)
	var uniqueVars []int
	for _, v := range consumedVars {
		if !seen[v] {
			seen[v] = true
			uniqueVars = append(uniqueVars, v)
		}
	}

	for i := 0; i < len(uniqueVars); i++ {
		for j := 0; j < len(uniqueVars)-1-i; j++ {
			if uniqueVars[j] < uniqueVars[j+1] {
				uniqueVars[j], uniqueVars[j+1] = uniqueVars[j+1], uniqueVars[j]
			}
		}
	}

	for _, idx := range uniqueVars {
		if idx < len(stmt.Vars) {
			stmt.Vars = append(stmt.Vars[:idx], stmt.Vars[idx+1:]...)
		}
	}
}

// 预编译正则表达式，性能更高
var mysqlJSONContainsRegex = regexp.MustCompile(
	`JSON_CONTAINS\s*\(\s*([^,]+?)\s*,\s*JSON_OBJECT\s*\(\s*(.+?)\s*\)\s*\)`,
)

// ConvertJSONContainsToPanWei 将 JSON_CONTAINS(col, JSON_OBJECT('k', $N, ...)) 转为内联 JSON
func ConvertJSONContainsToPanWei(sql string, stmt *gorm.Statement) (string, []int) {
	if sql == "" {
		return sql, nil
	}

	matches := mysqlJSONContainsRegex.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return sql, nil
	}

	offset := varOffset(stmt)
	var allConsumedVars []int
	result := sql

	kvRegex := regexp.MustCompile(`'([^']+)'\s*,\s*('([^']*)'|\$\d+)`)

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		col := strings.TrimSpace(match[1])
		paramsStr := strings.TrimSpace(match[2])

		kvMatches := kvRegex.FindAllStringSubmatch(paramsStr, -1)
		jsonObj := make(map[string]string)

		for _, kvm := range kvMatches {
			key := kvm[1]
			valStr := kvm[2]

			if strings.HasPrefix(valStr, "$") {
				n, err := strconv.Atoi(valStr[1:])
				if err == nil {
					varIndex := n - 1 + offset
					if varIndex < len(stmt.Vars) {
						jsonObj[key] = fmt.Sprint(stmt.Vars[varIndex])
						allConsumedVars = append(allConsumedVars, varIndex)
					}
				}
			} else {
				jsonObj[key] = strings.Trim(valStr, "'")
			}
		}

		jsonBytes, _ := json.Marshal(jsonObj)
		jsonStr := strings.ReplaceAll(string(jsonBytes), "'", "''")
		replacement := "JSON_CONTAINS(NULLIF(" + col + ", ''), '" + jsonStr + "')"

		result = strings.Replace(result, match[0], replacement, 1)
	}

	return result, allConsumedVars
}

// ConvertJSONContainsArray 将 JSON_CONTAINS(col, JSON_ARRAY($N)) 转为内联 JSON 数组
func ConvertJSONContainsArray(sql string, stmt *gorm.Statement) (string, []int) {
	if sql == "" {
		return sql, nil
	}

	containsArrayRegex := regexp.MustCompile(`JSON_CONTAINS\s*\(\s*([^,]+),\s*JSON_ARRAY\s*\(\s*(\$\d+)\s*\)\s*\)`)
	matches := containsArrayRegex.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return sql, nil
	}

	offset := varOffset(stmt)
	var allConsumedVars []int
	result := sql

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		col := strings.TrimSpace(match[1])
		paramStr := match[2]

		n, err := strconv.Atoi(paramStr[1:])
		if err != nil {
			continue
		}
		varIndex := n - 1 + offset
		if varIndex >= len(stmt.Vars) {
			continue
		}

		val := fmt.Sprint(stmt.Vars[varIndex])
		jsonArray := "[\"" + val + "\"]"
		jsonArray = strings.ReplaceAll(jsonArray, "'", "''")
		replacement := "JSON_CONTAINS(" + col + ", '" + jsonArray + "')"

		result = strings.Replace(result, match[0], replacement, 1)
		allConsumedVars = append(allConsumedVars, varIndex)
	}

	return result, allConsumedVars
}

// ConvertJSONContainsParam 将 JSON_CONTAINS(col_expr, $N) 裸参数转为内联值
func ConvertJSONContainsParam(sql string, stmt *gorm.Statement) (string, []int) {
	if sql == "" {
		return sql, nil
	}

	containsParamRegex := regexp.MustCompile(`JSON_CONTAINS\s*\(\s*(.+?),\s*(\$\d+)\s*\)`)
	matches := containsParamRegex.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return sql, nil
	}

	offset := varOffset(stmt)
	var allConsumedVars []int
	result := sql

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		col := strings.TrimSpace(match[1])
		paramStr := match[2]

		n, err := strconv.Atoi(paramStr[1:])
		if err != nil {
			continue
		}
		varIndex := n - 1 + offset
		if varIndex >= len(stmt.Vars) {
			continue
		}

		val := fmt.Sprint(stmt.Vars[varIndex])
		val = strings.ReplaceAll(val, "'", "''")
		replacement := "JSON_CONTAINS(" + col + ", '" + val + "')"

		result = strings.Replace(result, match[0], replacement, 1)
		allConsumedVars = append(allConsumedVars, varIndex)
	}

	return result, allConsumedVars
}
