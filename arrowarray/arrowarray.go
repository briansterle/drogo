package arrowarray

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type DroArray struct {
	arrow.Array
}

// impl ColumnVector for ArrowArray
func (arr DroArray) Len() int {
	return arr.Array.Len()
}

func (arr DroArray) GetValue(i int) any {
	panic("Not implemented")
}

func (arr DroArray) DataType() arrow.DataType {
	return arr.Array.DataType()
}

func Create(arrowType arrow.DataType, initialCapacity int, data []any) arrow.Array {
	rootAllocator := memory.NewGoAllocator()
	var arr arrow.Array
	switch arrowType.(type) {
	case *arrow.BooleanType:
		vs := array.NewBooleanBuilder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(bool))
		}
		arr = vs.NewArray()
	case *arrow.Int8Type:
		vs := array.NewInt8Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int8))
		}
		arr = vs.NewArray()
	case *arrow.Int16Type:
		vs := array.NewInt16Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int16))
		}
		arr = vs.NewArray()
	case *arrow.Int32Type:
		vs := array.NewInt32Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int32))
		}
		arr = vs.NewArray()
	case *arrow.Int64Type:
		vs := array.NewInt64Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int64))
		}
		arr = vs.NewArray()
	case *arrow.Float32Type:
		vs := array.NewFloat32Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(float32))
		}
		arr = vs.NewArray()
	case *arrow.Float64Type:
		vs := array.NewFloat64Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(float64))
		}
		arr = vs.NewArray()
	case *arrow.StringType:
		vs := array.NewStringBuilder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(string))
		}
		arr = vs.NewArray()
	default:
		panic("Unsupported Arrow type")
	}
	return DroArray{arr}
}
