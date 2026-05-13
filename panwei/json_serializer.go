package panwei

import (
	"context"
	"encoding/json"
	"reflect"

	"gorm.io/gorm/schema"
)

// JSONDefaultSerializer 纯 JSON 序列化器
type JSONDefaultSerializer struct{}

// fixInvalidJSON 修复无效的 JSON 字符串，移除换行符和制表符并转义双引号
func fixInvalidJSON(rawJSON string) string {

	if jsonRes, err := replaceQuotesInJSONValues(rawJSON); err == nil {
		return jsonRes
	}

	// jsonStr := strings.ReplaceAll(rawJSON, "\\\"", `\\"`)

	return rawJSON

}

// Value 写入数据库：绝对不出 Base64
func (JSONDefaultSerializer) Value(ctx context.Context, field *schema.Field, dst reflect.Value, fieldValue interface{}) (interface{}, error) {
	if fieldValue == nil {
		return nil, nil
	}

	// 直接返回原始字符串，禁止 json.Marshal（否则 []byte 自动变 Base64）
	switch v := fieldValue.(type) {
	case []byte:
		jsonStr := string(v)
		return fixInvalidJSON(jsonStr), nil
	case string:
		return fixInvalidJSON(v), nil
	case json.RawMessage:
		jsonStr := string(v)
		return fixInvalidJSON(jsonStr), nil
	default:
		// 使用反射检查是否是 json.RawMessage 的别名类型
		val := reflect.ValueOf(fieldValue)
		if val.Type().Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8 {
			// 是 []byte 或其别名类型
			jsonStr := string(val.Bytes())
			return fixInvalidJSON(jsonStr), nil
		}
		// 只有真·结构体才走这里
		data, err := json.Marshal(fieldValue)
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}
}

// Scan 读取数据库
func (JSONDefaultSerializer) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, dbValue interface{}) error {
	if dbValue == nil {
		return nil
	}

	var data []byte
	switch v := dbValue.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return nil
	}

	return json.Unmarshal(data, dst.Addr().Interface())
}
