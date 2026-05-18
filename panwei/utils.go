package panwei

import (
	"encoding/json"
)

func replaceQuotesInJSONValues(rawJSON string) (string, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(rawJSON), &data); err != nil {
		return "", err
	}

	// 重新编码为 JSON，让 json.Marshal 自动处理转义
	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
