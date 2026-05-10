package zeronull_test

import (
	"context"
	"testing"

	"github.com/lwplvx/gorm-dm/panwei/gaussdb-go/gaussdbtype/zeronull"
	"github.com/lwplvx/gorm-dm/panwei/gaussdb-go/gaussdbxtest"
)

func TestTextTranscode(t *testing.T) {
	gaussdbxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "text", []gaussdbxtest.ValueRoundTripTest{
		{
			(zeronull.Text)("foo"),
			new(zeronull.Text),
			isExpectedEq((zeronull.Text)("foo")),
		},
		{
			nil,
			new(zeronull.Text),
			isExpectedEq((zeronull.Text)("")),
		},
		{
			(zeronull.Text)(""),
			new(any),
			isExpectedEq(nil),
		},
	})
}
