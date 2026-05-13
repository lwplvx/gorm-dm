package gaussdbgo

// 声明一个函数变量，用于接收 盘维数据库处理 json 字符串中的双引号转义问题的函数
var InterceptSqlArgumentVal func(s string) string

func InterceptGaussdbArgumentVal(s string) string {

	if InterceptSqlArgumentVal != nil {
		// 如果没有设置 InterceptSqlArgumentVal 函数，则直接返回原始值 s
		return InterceptSqlArgumentVal(s)
	}

	return s
}
