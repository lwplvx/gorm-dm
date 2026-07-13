package panwei

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	panweidbgo "github.com/lwplvx/gorm-dm/panwei/gaussdb-go"
	"github.com/lwplvx/gorm-dm/panwei/gaussdb-go/stdlib"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Dialector struct {
	*Config
}

type Config struct {
	DriverName               string
	DSN                      string
	WithoutQuotingCheck      bool
	PreferSimpleProtocol     bool
	WithoutReturning         bool
	Conn                     gorm.ConnPool
	DisabledJsonArrayAsJsonb bool
	BatchIDBackfillStrategy  string // 批次 ID 回填策略：maxid / fieldquery（默认 fieldquery）
}

var (
	timeZoneMatcher         = regexp.MustCompile("(time_zone|TimeZone)=(.*?)($|&| )")
	defaultIdentifierLength = 63 //maximum identifier length for gaussdb
)

// 批量 INSERT 后 ID 回填策略（解决 ON CONFLICT + 无 RETURNING 时的 ID 获取问题）
const (
	BatchIDBackfillMaxID      = "maxid"      // 方向1: SELECT MAX(id) 反算批次 ID（性能好，依赖 ID 连续）
	BatchIDBackfillFieldQuery = "fieldquery" // 方向2: 按非主键字段逐条回查（更鲁棒，每元素多一次查询）
)

func Open(dsn string) gorm.Dialector {
	config := &Config{DSN: dsn}

	SetDefaultConfig(config)
	return &Dialector{Config: config}
}

// 配置变化时需要  Open New 两个函数都一起处理
func New(config Config) gorm.Dialector {
	SetDefaultConfig(&config)
	return &Dialector{Config: &config}
}

// 是否启用 强转jsonb 查询, 默认启用
var enabledJsonArrayAsJsonb = true

// 配置变化时需要  Open New 两个函数都一起处理
func SetDefaultConfig(config *Config) {
	config.PreferSimpleProtocol = true

	// 默认使用 fieldquery 策略（更鲁棒）
	if config.BatchIDBackfillStrategy == "" {
		config.BatchIDBackfillStrategy = BatchIDBackfillFieldQuery
	}

	// 是否启用 强转jsonb 查询,默认启用
	enabledJsonArrayAsJsonb = true
	if config.DisabledJsonArrayAsJsonb {
		enabledJsonArrayAsJsonb = false
	}
}

func (dialector Dialector) Name() string {
	return "panwei"
}

func (dialector Dialector) Apply(config *gorm.Config) error {
	if config.NamingStrategy == nil {
		config.NamingStrategy = schema.NamingStrategy{
			IdentifierMaxLength: defaultIdentifierLength,
		}
		return nil
	}

	switch v := config.NamingStrategy.(type) {
	case *schema.NamingStrategy:
		if v.IdentifierMaxLength <= 0 {
			v.IdentifierMaxLength = defaultIdentifierLength
		}
	case schema.NamingStrategy:
		if v.IdentifierMaxLength <= 0 {
			v.IdentifierMaxLength = defaultIdentifierLength
			config.NamingStrategy = v
		}
	}

	return nil
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	callbackConfig := &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "MERGE", "ON CONFLICT"},
		UpdateClauses: []string{"UPDATE", "SET", "FROM", "WHERE"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE"},
	}
	// register callbacks
	if !dialector.WithoutReturning {
		callbackConfig.CreateClauses = append(callbackConfig.CreateClauses, "RETURNING")
		callbackConfig.UpdateClauses = append(callbackConfig.UpdateClauses, "RETURNING")
		callbackConfig.DeleteClauses = append(callbackConfig.DeleteClauses, "RETURNING")
	}
	callbacks.RegisterDefaultCallbacks(db, callbackConfig)

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else if dialector.DriverName != "" {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.Config.DSN)
	} else {
		var config *panweidbgo.ConnConfig

		config, err = panweidbgo.ParseConfig(dialector.Config.DSN)
		if err != nil {
			return
		}

		//  当前是兼容模式，一定要启用
		dialector.Config.PreferSimpleProtocol = true
		if dialector.Config.PreferSimpleProtocol {
			// 代理/兼容场景 适合这种模式
			config.DefaultQueryExecMode = panweidbgo.QueryExecModeSimpleProtocol
			fmt.Printf("打印 DefaultQueryExecMode 参数 0: %v\n", config.DefaultQueryExecMode)
		}

		result := timeZoneMatcher.FindStringSubmatch(dialector.Config.DSN)
		if len(result) > 2 {
			config.RuntimeParams["timezone"] = result[2]
		}
		db.ConnPool = stdlib.OpenDB(*config)
	}
	for k, v := range dialector.ClauseBuilders() {
		if _, ok := db.ClauseBuilders[k]; !ok {
			db.ClauseBuilders[k] = v
		}
	}

	// 替换默认的 Create 回调函数，支持 INSERT ON DUPLICATE KEY UPDATE 后获取自增 ID
	ExtendCreateCallback(db, dialector.Config.BatchIDBackfillStrategy)

	RegisterDefaultSqlArgIntercepter()

	return
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('$')
	index := 0
	varLen := len(stmt.Vars)
	if varLen > 0 {
		switch stmt.Vars[0].(type) {
		case panweidbgo.QueryExecMode:
			index++
		}
	}
	writer.WriteString(strconv.Itoa(varLen - index))
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	if dialector.WithoutQuotingCheck {
		writer.WriteString(str)
		return
	}

	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '"':
			continuousBacktick++
			if continuousBacktick == 2 {
				writer.WriteString(`""`)
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				writer.WriteByte('"')
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				writer.WriteByte('"')
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				writer.WriteString(`""`)
			}

			writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		writer.WriteString(`""`)
	}
	writer.WriteByte('"')
}

var numericPlaceholder = regexp.MustCompile(`\$(\d+)`)

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "smallint"
	case schema.Int, schema.Uint:
		size := field.Size
		if field.DataType == schema.Uint {
			size++
		}
		if field.AutoIncrement {
			switch {
			case size <= 16:
				return "smallserial"
			case size <= 32:
				return "serial"
			default:
				return "bigserial"
			}
		} else {
			switch {
			case size <= 16:
				return "smallint"
			case size <= 32:
				return "integer"
			default:
				return "bigint"
			}
		}
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("numeric(%d, %d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("numeric(%d)", field.Precision)
		}
		return "decimal"
	case schema.String:
		if field.Size > 0 && field.Size <= 10485760 {
			return fmt.Sprintf("varchar(%d)", field.Size)
		}
		return "text"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("timestamptz(%d)", field.Precision)
		}
		return "timestamptz"
	case schema.Bytes:
		return "bytea"
	default:
		return dialector.getSchemaCustomType(field)
	}
}

func (dialector Dialector) getSchemaCustomType(field *schema.Field) string {
	sqlType := string(field.DataType)

	if field.AutoIncrement && !strings.Contains(strings.ToLower(sqlType), "serial") {
		size := field.Size
		if field.GORMDataType == schema.Uint {
			size++
		}
		switch {
		case size <= 16:
			sqlType = "smallserial"
		case size <= 32:
			sqlType = "serial"
		default:
			sqlType = "bigserial"
		}
	}

	return sqlType
}

func (dialector Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dialector Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}

const (
	// ClauseOnConflict for clause.ClauseBuilder ON CONFLICT key
	ClauseOnConflict = "ON CONFLICT"
)

func (dialector Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	clauseBuilders := map[string]clause.ClauseBuilder{
		"RETURNING": func(c clause.Clause, builder clause.Builder) {
			// 检查是否是 INSERT ON DUPLICATE KEY UPDATE 语句
			// 如果是，已在 ClauseOnConflict 中处理了 ID 获取
			// 需要同时从 Clauses 中移除 RETURNING 子句，否则 hasReturning() 误判为 true
			// 导致 GORM 标准 Create 回调走 QueryContext 路径而不是 ExecContext 路径，
			// 进而导致 LastInsertId() 不可用、ExtendCreateCallback 的 ID 回填被跳过。
			stmt := builder.(*gorm.Statement)
			for _, cl := range stmt.Clauses {
				if cl.Name == "ON CONFLICT" {
					delete(stmt.Clauses, "RETURNING")
					return
				}
			}
			// 否则，正常处理 RETURNING 子句
			c.Build(builder)
		},

		ClauseOnConflict: func(c clause.Clause, builder clause.Builder) {
			onConflict, ok := c.Expression.(clause.OnConflict)
			if !ok {
				c.Build(builder)
				return
			}
			if len(onConflict.DoUpdates) == 0 {
				if s := builder.(*gorm.Statement).Schema; s != nil {
					var column clause.Column
					if s.PrioritizedPrimaryField != nil {
						column = clause.Column{Name: s.PrioritizedPrimaryField.DBName}
					} else if len(s.DBNames) > 0 {
						column = clause.Column{Name: s.DBNames[0]}
					}
					if column.Name != "" && !isPrimaryOrUniqueKey(builder, column.Name) {
						onConflict.DoUpdates = []clause.Assignment{{Column: column, Value: column}}
						onConflict.DoNothing = false
					}
					builder.(*gorm.Statement).AddClause(onConflict)
				}
			}

			notFirstField := false
			hasWritten := false
			for _, assignment := range onConflict.DoUpdates {
				if isPrimaryOrUniqueKey(builder, assignment.Column.Name) {
					continue
				}
				if !hasWritten {
					builder.WriteString("ON DUPLICATE KEY UPDATE ")
					hasWritten = true
				}
				if notFirstField {
					builder.WriteByte(',')
				}
				builder.WriteQuoted(assignment.Column)
				builder.WriteByte('=')
				if column, ok := assignment.Value.(clause.Column); ok && column.Table == "excluded" {
					column.Table = ""
					builder.WriteString("VALUES(")
					builder.WriteQuoted(column)
					builder.WriteByte(')')
				} else {
					builder.AddVar(builder, assignment.Value)
				}
				notFirstField = true
			}
			if !hasWritten {
				builder.WriteString("ON DUPLICATE KEY UPDATE NOTHING")
			}
			// 不添加 RETURNING 子句，因为 Panwei 数据库不支持在 INSERT ON DUPLICATE KEY UPDATE 语句中使用 RETURNING 子句
			// 而是在 replaceCreateCallback 函数中通过额外的 SELECT 语句获取自增 ID
		},

		"SELECT": func(c clause.Clause, builder clause.Builder) {
			if values, ok := c.Expression.(clause.Select); ok && len(values.Columns) > 0 {
				if stmt, ok := builder.(*gorm.Statement); ok {
					if len(stmt.Selects) == 0 {
						c.Build(builder)
						return
					}
					isFloor := false
					// 循环 stmt.Selects 进行处理
					for idx, selectStr := range stmt.Selects {
						if strings.Contains(selectStr, "FLOOR(") && strings.Contains(selectStr, "AS ts") {

							isFloor = true
							exprStr := ConvertFloorToCast(selectStr)
							// 如果 idx == 0
							if idx == 0 {
								builder.WriteString("SELECT " + exprStr)
							}
						}
					}
					if !isFloor {
						c.Build(builder)
					}
				} else {
					c.Build(builder)
				}
			} else {
				c.Build(builder)
			}
		},
	}

	// 合并 JSON 子句构建器
	for key, builder := range GetJSONClauseBuilders() {
		clauseBuilders[key] = builder
	}

	return clauseBuilders
}
func getSerialDatabaseType(s string) (dbType string, ok bool) {
	switch s {
	case "smallserial":
		return "smallint", true
	case "serial":
		return "integer", true
	case "bigserial":
		return "bigint", true
	default:
		return "", false
	}
}

func isPrimaryOrUniqueKey(builder clause.Builder, name string) bool {
	s := builder.(*gorm.Statement).Schema
	if s == nil || name == "" {
		return false
	}
	if s.PrimaryFields != nil {
		for _, field := range s.PrimaryFields {
			if field.DBName == name && (field.PrimaryKey || field.Unique) {
				return true
			}
		}
	}
	return false
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
				replaceMysqlSqlToPanWeiSql(builder)
				return
			}
			// 默认处理
			c.Build(builder)
			// 执行 SQL 替换
			replaceMysqlSqlToPanWeiSql(builder)
		},
	}
}

// 替换默认的 Create 回调函数，支持 INSERT ON DUPLICATE KEY UPDATE 后获取自增 ID
func ExtendCreateCallback(db *gorm.DB, strategy string) {
	// 保存原始的 Create 回调函数
	originalCreate := db.Callback().Create().Get("gorm:create")
	if originalCreate == nil {
		return
	}

	// 注册新的 Create 回调函数
	db.Callback().Create().Replace("gorm:create", func(db *gorm.DB) {
		// 执行原始的 Create 回调函数
		originalCreate(db)

		// 如果有错误，直接返回
		if db.Error != nil {
			return
		}

		// 检查是否使用了 INSERT ON DUPLICATE KEY UPDATE 语句
		if _, ok := db.Statement.Clauses["ON CONFLICT"]; ok {

			// 检查是否有自增主键
			if db.Statement.Schema != nil && db.Statement.Schema.PrioritizedPrimaryField != nil && db.Statement.Schema.PrioritizedPrimaryField.AutoIncrement {
				// 构建查询最新 ID 的 SQL 语句
				var id interface{}

				queryBuilder := strings.Builder{}
				if db.Statement.Schema.PrioritizedPrimaryField != nil && db.Statement.Schema.PrioritizedPrimaryField.AutoIncrement {
					queryBuilder.WriteString("SELECT ")
					queryBuilder.WriteString(db.Statement.Quote(db.Statement.Schema.PrioritizedPrimaryField.DBName))
					queryBuilder.WriteString(" FROM ")
					queryBuilder.WriteString(db.Statement.Quote(db.Statement.Table))
					queryBuilder.WriteString(" ORDER BY ")
					queryBuilder.WriteString(db.Statement.Quote(db.Statement.Schema.PrioritizedPrimaryField.DBName))
					queryBuilder.WriteString(" DESC LIMIT 1")
				}
				query := queryBuilder.String()

				// 直接使用 ConnPool 执行查询，避免参数复用问题
				if err := db.Statement.ConnPool.QueryRowContext(db.Statement.Context, query).Scan(&id); err == nil {
					// 将获取到的 ID 回填到结构体中
					if field := db.Statement.Schema.PrioritizedPrimaryField; field != nil && db.Statement.ReflectValue.IsValid() {
						kind := db.Statement.ReflectValue.Kind()
						if kind == reflect.Slice || kind == reflect.Array {
							// 根据策略选择回填方式
							switch strategy {
							case BatchIDBackfillMaxID:
								backfillBatchIDsByMaxID(db, field, id)
							default:
								backfillBatchIDsByFields(db, field)
							}
						} else {
							// 单条 INSERT：直接回填
							rv := db.Statement.ReflectValue
							if rv.CanAddr() { // 必须可寻址才能 Set
								field.Set(db.Statement.Context, rv, id)
							}
						}
					}
				}
			}
		}

	})
}

// backfillBatchIDsByFields 方向2：批量 INSERT 后按字段回查各记录的 ID
// 遍历切片中每个元素，用非主键字段值构建 WHERE，从数据库查出实际 ID 并回填。
// 不受 ON CONFLICT DO NOTHING 影响（已存在记录也能查出正确 ID）。
func backfillBatchIDsByFields(db *gorm.DB, pkField *schema.Field) {
	schema := db.Statement.Schema
	if schema == nil {
		return
	}
	length := db.Statement.ReflectValue.Len()

	for i := 0; i < length; i++ {
		elem := db.Statement.ReflectValue.Index(i)
		if !elem.CanAddr() {
			continue
		}
		// 用非主键、非自增的可读字段构建 WHERE 条件
		var conds []string
		var vals []interface{}
		paramIdx := 1
		for _, f := range schema.Fields {
			if f.PrimaryKey || f.AutoIncrement || !f.Readable {
				continue
			}
			// 跳过关联字段（HasMany/HasOne/BelongsTo/Many2Many），它们不是数据库列
			if f.DBName == "" {
				continue
			}
			fv, isZero := f.ValueOf(db.Statement.Context, elem)
			if isZero {
				continue
			}
			conds = append(conds, fmt.Sprintf("%s = $%d",
				db.Statement.Quote(f.DBName), paramIdx))
			vals = append(vals, fv)
			paramIdx++
		}
		if len(conds) == 0 {
			continue
		}

		// SELECT "id" FROM "languages" WHERE "name" = $1 LIMIT 1
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1",
			db.Statement.Quote(pkField.DBName),
			db.Statement.Quote(schema.Table),
			strings.Join(conds, " AND "))

		var id interface{}
		if err := db.Statement.ConnPool.QueryRowContext(db.Statement.Context, q, vals...).Scan(&id); err != nil {
			continue // 查询失败保持 ID=0，与原始跳过行为一致
		}
		pkField.Set(db.Statement.Context, elem, id)
	}
}

// backfillBatchIDsByMaxID 方向1：从 SELECT MAX(id) 结果反算批次内各记录的 ID
// 性能好（只需一次额外查询），但要求批次内 ID 连续且全部为新插入。
func backfillBatchIDsByMaxID(db *gorm.DB, pkField *schema.Field, maxID interface{}) {
	length := db.Statement.ReflectValue.Len()
	if length == 0 {
		return
	}
	// 将 maxID 转为 int64
	var maxVal int64
	switch v := maxID.(type) {
	case int64:
		maxVal = v
	case float64:
		maxVal = int64(v)
	case int:
		maxVal = int64(v)
	default:
		fmt.Printf("[backfill-maxid] unexpected maxID type: %T, skipping\n", maxID)
		return
	}
	if maxVal < int64(length) {
		fmt.Printf("[backfill-maxid] maxVal %d < length %d, skipping\n", maxVal, length)
		return
	}
	// firstID = maxID - len + 1, 然后依次赋值
	firstID := uint(maxVal) - uint(length) + 1
	fmt.Printf("[backfill-maxid] maxID=%d, len=%d, firstID=%d\n", maxVal, length, firstID)
	for i := 0; i < length; i++ {
		elem := db.Statement.ReflectValue.Index(i)
		if elem.CanAddr() {
			pkField.Set(db.Statement.Context, elem, firstID+uint(i))
		}
	}
}

// 匹配：FLOOR(字段/数字)*数字
var floorPattern = regexp.MustCompile(`\bFLOOR\s*\(\s*[a-zA-Z_][a-zA-Z0-9_]*\s*\/\s*\d+\s*\)\s*\*\s*\d+\b`)

// ConvertFloorToCast 安全替换：只替换未被 CAST 包裹的表达式
func ConvertFloorToCast(sql string) string {
	// 先把所有已经是 CAST(...) 的内容标记为占位符，防止被二次替换
	// 步骤1：替换已存在的 CAST(...) 为临时标记
	castedRegex := regexp.MustCompile(`CAST\(.*?AS BIGINT\)`)
	tempMap := make(map[string]string)
	tempIndex := 0

	// 保存已存在的CAST语句
	sql = castedRegex.ReplaceAllStringFunc(sql, func(m string) string {
		key := sprintfHelper("__CAST_PLACEHOLDER_%d__", tempIndex)
		tempMap[key] = m
		tempIndex++
		return key
	})

	// 步骤2：替换所有 FLOOR 表达式
	sql = floorPattern.ReplaceAllStringFunc(sql, func(m string) string {
		return "CAST(" + m + " AS BIGINT)"
	})

	// 步骤3：还原占位符
	for key, val := range tempMap {
		sql = strings.ReplaceAll(sql, key, val)
	}

	return sql
}

// 辅助函数
func sprintfHelper(format string, a ...interface{}) string {
	return strings.ReplaceAll(format, "%d", fmt.Sprint(a[0]))
}
