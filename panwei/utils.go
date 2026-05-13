package panwei

import (
	"encoding/json"
	"strings"
)

func replaceQuotesInJSONValues(rawJSON string) (string, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(rawJSON), &data); err != nil {
		return "", err
	}

	// 递归处理
	processValue(&data)

	// 重新编码为 JSON
	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	s := string(bytes)

	return s, nil
}

// 递归地将所有字符串中的 " 替换为 \"
func processValue(v *interface{}) {
	switch val := (*v).(type) {
	case map[string]interface{}:
		for k, v2 := range val {
			processValue(&v2)
			val[k] = v2
		}
	case []interface{}:
		for i, v2 := range val {
			processValue(&v2)
			val[i] = v2
		}
	case string:
		*v = strings.ReplaceAll(val, `"`, `\"`)
	}
}
