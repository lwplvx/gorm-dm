package gaussdbgo

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/lwplvx/gorm-dm/panwei/gaussdb-go/gaussdbtype"
	"github.com/lwplvx/gorm-dm/panwei/gaussdb-go/internal/gaussdbio"
)

// GaussDB format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

func convertSimpleArgument(m *gaussdbtype.Map, arg any) (any, error) {
	buf, err := m.Encode(0, TextFormatCode, arg, []byte{})
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	// s := string(buf)
	s := convertParamsArgument(arg, buf)

	// 盘维数据库处理 json 字符串中的双引号转义问题
	s = InterceptGaussdbArgumentVal(s)

	return s, nil
}

// 盘维数据库处理  int 类型的 枚举 带有string() 的时候 被错误转换成string
func convertParamsArgument(v any, buf []byte) string {
	// 取值实现参考的是 gorm.io\gorm@v1.30.1\logger\sql.go  的 convertParams
	//  发现其他类型转换错误，可以 继续参考 convertParams 做补充

	switch v := v.(type) {
	// case bool:
	// 	arg = strconv.FormatBool(v)

	case fmt.Stringer:
		reflectValue := reflect.ValueOf(v)
		switch reflectValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			arg := fmt.Sprintf("%d", reflectValue.Interface())
			return arg
		}
	}
	return string(buf)
}

func encodeCopyValue(m *gaussdbtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	sp := len(buf)
	buf = gaussdbio.AppendInt32(buf, -1)
	argBuf, err := m.Encode(oid, BinaryFormatCode, arg, buf)
	if err != nil {
		if argBuf2, err2 := tryScanStringCopyValueThenEncode(m, buf, oid, arg); err2 == nil {
			argBuf = argBuf2
		} else {
			return nil, err
		}
	}

	if argBuf != nil {
		buf = argBuf
		gaussdbio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
	}
	return buf, nil
}

func tryScanStringCopyValueThenEncode(m *gaussdbtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	s, ok := arg.(string)
	if !ok {
		textBuf, err := m.Encode(oid, TextFormatCode, arg, nil)
		if err != nil {
			return nil, errors.New("not a string and cannot be encoded as text")
		}
		s = string(textBuf)
	}

	var v any
	err := m.Scan(oid, TextFormatCode, []byte(s), &v)
	if err != nil {
		return nil, err
	}

	return m.Encode(oid, BinaryFormatCode, v, buf)
}
