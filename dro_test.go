package drogo

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/briansterle/drogo/util"
	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {

	// bool
	data := util.SliceToAny([]bool{true, false, true})
	arr := New(&arrow.BooleanType{}, 10, data)
	assert.Equal(t, true, arr.GetValue(0), "should equal bool")
	assert.Equal(t, false, arr.GetValue(1), "should equal bool")
	assert.Equal(t, true, arr.GetValue(2), "should equal bool")
	assert.Equal(t, "[true false true]", arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, Boolean, arr.DataType(), "should equal type")

	// string
	data = util.SliceToAny([]string{"a", "b", "c"})
	arr = New(&arrow.StringType{}, 10, data)
	assert.Equal(t, "a", arr.GetValue(0), "should equal string")
	assert.Equal(t, "b", arr.GetValue(1), "should equal string")
	assert.Equal(t, "c", arr.GetValue(2), "should equal string")
	assert.Equal(t, `["a" "b" "c"]`, arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, String, arr.DataType(), "should equal type")

	// int64
	data = util.SliceToAny([]int64{1, 2, 3})
	arr = New(&arrow.Int64Type{}, 10, data)
	assert.Equal(t, int64(1), arr.GetValue(0), "should equal int 64")
	assert.Equal(t, int64(2), arr.GetValue(1), "should equal int 64")
	assert.Equal(t, int64(3), arr.GetValue(2), "should equal int 64")
	assert.Equal(t, "[1 2 3]", arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, Int64, arr.DataType(), "should equal type")

	// int32
	data = util.SliceToAny([]int32{1, 2, 3})
	arr = New(&arrow.Int32Type{}, 10, data)
	assert.Equal(t, int32(1), arr.GetValue(0), "should equal int32")
	assert.Equal(t, int32(2), arr.GetValue(1), "should equal int32")
	assert.Equal(t, int32(3), arr.GetValue(2), "should equal int32")
	assert.Equal(t, "[1 2 3]", arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, Int32, arr.DataType(), "should equal type")

	// int16
	data = util.SliceToAny([]int16{1, 2, 3})
	arr = New(&arrow.Int16Type{}, 10, data)
	assert.Equal(t, int16(1), arr.GetValue(0), "should equal int16")
	assert.Equal(t, int16(2), arr.GetValue(1), "should equal int16")
	assert.Equal(t, int16(3), arr.GetValue(2), "should equal int16")
	assert.Equal(t, "[1 2 3]", arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, Int16, arr.DataType(), "should equal type")

	// int8
	data = util.SliceToAny([]int8{1, 2, 3})
	arr = New(&arrow.Int8Type{}, 10, data)
	assert.Equal(t, int8(1), arr.GetValue(0), "should equal int8")
	assert.Equal(t, int8(2), arr.GetValue(1), "should equal int8")
	assert.Equal(t, int8(3), arr.GetValue(2), "should equal int8")
	assert.Equal(t, "[1 2 3]", arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, Int8, arr.DataType(), "should equal type")

	// float64
	data = util.SliceToAny([]float64{1.1, 2.2, 3.3})
	arr = New(&arrow.Float64Type{}, 10, data)
	assert.Equal(t, 1.1, arr.GetValue(0), "should equal float64")
	assert.Equal(t, 2.2, arr.GetValue(1), "should equal float64")
	assert.Equal(t, 3.3, arr.GetValue(2), "should equal float64")
	assert.Equal(t, "[1.1 2.2 3.3]", arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, Float64, arr.DataType(), "should equal type")

	// float32
	data = util.SliceToAny([]float32{1.1, 2.2, 3.3})
	arr = New(&arrow.Float32Type{}, 10, data)
	assert.Equal(t, float32(1.1), arr.GetValue(0), "should equal float32")
	assert.Equal(t, float32(2.2), arr.GetValue(1), "should equal float32")
	assert.Equal(t, float32(3.3), arr.GetValue(2), "should equal float32")
	assert.Equal(t, "[1.1 2.2 3.3]", arr.String(), "should equal string")
	assert.Equal(t, 3, arr.Len(), "should equal length")
	assert.Equal(t, Float32, arr.DataType(), "should equal type")

}
