package engine

import (
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/briansterle/drogo"
)

type PhysicalPlan interface {
	Schema() Schema
	Execute() []RecordBatch // should return an iterator?
	Children() []PhysicalPlan
}

type Expression interface {
	Evaluate(input RecordBatch) ColumnVector
}

type ColumnExpression struct {
	i int
}

func (col ColumnExpression) Evaluate(input RecordBatch) ColumnVector {
	return input.Field(col.i)
}

func (col ColumnExpression) String() string {
	return "#" + strconv.Itoa(col.i)
}

type LiteralInt64Expression struct {
	value int64
}

func (lit LiteralInt64Expression) Evaluate(input RecordBatch) ColumnVector {
	return LiteralValueVector{drogo.Int64, lit.value, input.RowCount()}
}

type LiteralFloat64Expression struct {
	value float64
}

func (lit LiteralFloat64Expression) Evaluate(input RecordBatch) ColumnVector {
	return LiteralValueVector{drogo.Float64, lit.value, input.RowCount()}
}

type LiteralStringExpression struct {
	value string
}

func (lit LiteralStringExpression) Evaluate(input RecordBatch) ColumnVector {
	return LiteralValueVector{drogo.String, lit.value, input.RowCount()}
}

type BinaryExpression struct {
	l Expression
	r Expression
	BinaryExpressionEvaluator
}

type BinaryExpressionEvaluator interface {
	Evaluate(input RecordBatch) ColumnVector
	evaluate(l, r ColumnVector) ColumnVector
}

func (e BinaryExpression) Evaluate(input RecordBatch) ColumnVector {
	ll := e.l.Evaluate(input)
	rr := e.r.Evaluate(input)
	if ll.Len() != rr.Len() {
		panic("Binary expression operands do not have the same size")
	}
	if ll.DataType() != rr.DataType() {
		panic("Binary expression operands do not have the same type")
	}
	return e.evaluate(ll, rr)
}

func (e BinaryExpression) evaluate(l, r ColumnVector) ColumnVector {
	return e.BinaryExpressionEvaluator.evaluate(l, r)
}

type MathExpression struct {
	l Expression
	r Expression
	MathExpressionEvaluator
}

type MathExpressionEvaluator interface {
	Expression
	evaluate(l any, r any, arrowType arrow.DataType) any
}

func (e MathExpression) Evaluate(l ColumnVector, r ColumnVector) ColumnVector {
	values := make([]any, l.Len())
	for i := 0; i < l.Len(); i++ {
		value := e.evaluate(l.GetValue(i), r.GetValue(i), l.DataType())
		values[i] = value
	}

	return drogo.New(l.DataType(), l.Len(), values)
}
