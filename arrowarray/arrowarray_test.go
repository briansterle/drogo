package arrowarray

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/stretchr/testify/assert"
)

func toAny[T any](data []T) []any {
	anyData := make([]any, len(data))
	for i, v := range data {
		anyData[i] = v
	}
	return anyData
}

func TestCreate(t *testing.T) {
	data := toAny([]int64{1, 2, 3})
	arr := Create(&arrow.Int64Type{}, 10, data)
	asInt64 := *array.NewInt64Data(arr.Data())
	assert.True(t, asInt64.Value(0) == 1, "should equal int 64")
	assert.True(t, asInt64.Value(1) == 2, "should equal int 64")
	assert.True(t, asInt64.Value(2) == 3, "should equal int 64")

}
