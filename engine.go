package main

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
)

type ColumnVector interface {
	getType() arrow.Type
	getValue(i int) interface{}
	getSize() int
}

type LiteralValueVector struct {
	arrowType arrow.Type
	value     interface{}
	size      int
}

func (v *LiteralValueVector) getType() arrow.Type {
	return v.arrowType
}

func (v *LiteralValueVector) getValue(i int) (interface{}, error) {
	if i < 0 || i >= v.size {
		return nil, fmt.Errorf("Index out of bounds %d vecsize: %d", i, v.size)
	}
	return v.value, nil
}

func (v *LiteralValueVector) getSize() int {
	return v.size
}

type RecordBatch struct {
	schema arrow.Schema
	fields []ColumnVector
}

func (r *RecordBatch) rowCount() int {
	return r.fields[0].getSize()
}

func (r *RecordBatch) columnCount() int {
	return len(r.fields)
}

func (r *RecordBatch) field(i int) ColumnVector {
	return r.fields[i]
}
