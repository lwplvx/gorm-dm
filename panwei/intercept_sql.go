package panwei

import (
	"strings"

	panweidbgo "github.com/lwplvx/gorm-dm/panwei/gaussdb-go"
)

var PanweiSqlArgIntercepter func(s string) string

func RegisterSqlArgIntercepter(sqlArgIntercepterCallback func(s string) string) {

	// 这里注册一个回调函数，在执行 SQL 之前对参数进行处理
	PanweiSqlArgIntercepter = sqlArgIntercepterCallback

}

func RegisterDefaultSqlArgIntercepter() {
	panweidbgo.InterceptSqlArgumentVal = InterceptSqlArgumentVal
}

func InterceptSqlArgumentVal(s string) string {

	// 如果外部有注册这个函数，就调用外部注册的函数进行处理，否则执行默认的处理逻辑
	if PanweiSqlArgIntercepter != nil {
		return PanweiSqlArgIntercepter(s)
	}

	if strings.Contains(s, "{") && strings.Contains(s, ":") && strings.Contains(s, "}") {
		// println("[PANWEI] --1--替换前的 Value: " + s)

		// 这里对json 进行处理， 替换 \" 为 \\\"
		s = strings.ReplaceAll(s, "\"", "\\\"")

		// 根据日志级别进行日志输出， 这里假设日志级别为 Info
		// println("[PANWEI]  ---2---替换后的 Value: " + s)
	}

	return s

}
