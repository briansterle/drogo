package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/briansterle/drogo"
)

type PhysicalPlan interface {
	GetSchema() Schema
	Execute() []RecordBatch // should return an iterator?
	Children() []PhysicalPlan
}

type Expression interface {
	Evaluate(input RecordBatch) ColumnVector
	String() string
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

func (col ColumnExpression) ToField(plan LogicalPlan) arrow.Field {
	return plan.Schema().Field(col.i)
}

type LiteralInt64Expression struct {
	value int64
}

func (lit LiteralInt64Expression) String() string {
	return strconv.FormatInt(lit.value, 10)
}

func (lit LiteralInt64Expression) Evaluate(input RecordBatch) ColumnVector {
	return LiteralValueVector{drogo.Int64, lit.value, input.RowCount()}
}

type LiteralFloat64Expression struct {
	value float64
}

func (lit LiteralFloat64Expression) String() string {
	return strconv.FormatFloat(lit.value, 'f', -1, 64)
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

type AddExpression struct {
	MathExpression
}

func (e AddExpression) Evaluate(l any, r any, arrowType arrow.DataType) any {
	switch arrowType {
	case drogo.Int64:
		return l.(int64) + r.(int64)
	case drogo.Int32:
		return l.(int32) + r.(int32)
	case drogo.Int16:
		return l.(int16) + r.(int16)
	case drogo.Int8:
		return l.(int8) + r.(int8)
	case drogo.Float64:
		return l.(float64) + r.(float64)
	case drogo.Float32:
		return l.(float32) + r.(float32)
	default:
		panic("unsupported type")
	}
}

func (e AddExpression) String() string {
	return e.l.String() + "+" + e.r.String()
}

type AggregateExpression interface {
	InputExpression() Expression
	CreateAccumulator() Accumulator
}

type Accumulator interface {
	Accumulate(value any)
	FinalValue() any
}

type MaxExpression struct {
	expr Expression
}

// impl aggregate expression
func (e MaxExpression) InputExpression() Expression {
	return e.expr
}

func (e MaxExpression) CreateAccumulator() Accumulator {
	return &MaxAccumulator{}
}

func (e MaxExpression) String() string {
	return "MAX(" + e.expr.String() + ")"
}

type MaxAccumulator struct {
	value any
}

func (a *MaxAccumulator) Accumulate(value any) {
	if a.value == nil {
		a.value = value
		return
	}
	switch value.(type) {
	case int8:
		if a.value.(int8) < value.(int8) {
			a.value = value
		}
	case int16:
		if a.value.(int16) < value.(int16) {
			a.value = value
		}
	case int32:
		if a.value.(int32) < value.(int32) {
			a.value = value
		}
	case int64:
		if a.value.(int64) < value.(int64) {
			a.value = value
		}
	case float64:
		if a.value.(float64) < value.(float64) {
			a.value = value
		}
	case float32:
		if a.value.(float32) < value.(float32) {
			a.value = value
		}
	default:
		panic("unsupported type")
	}
}

func (a *MaxAccumulator) FinalValue() any {
	return a.value
}

// todo implement other physical expressions

// ScanExec is a PhysicalPlan that simply delegates to a datasource
type ScanExec struct {
	DataSource DataSource
	Projection []string
}

func (s ScanExec) Schema() Schema {
	return s.DataSource.GetSchema().Select(s.Projection)
}

func (s ScanExec) Execute() []RecordBatch {
	return s.DataSource.Scan(s.Projection)
}

func (s ScanExec) Children() []PhysicalPlan {
	return []PhysicalPlan{}
}

func (s ScanExec) String() string {
	return "ScanExec: schema=" + s.Schema().String() +
		", projection=" + strings.Join(s.Projection, ",")
}

// ProjectionExec simply evaluates the projection expressions and produces
// a record batch with the derived columns
type ProjectionExec struct {
	Input  PhysicalPlan
	Schema Schema
	Exprs  []Expression
}

func (p ProjectionExec) String() string {
	return fmt.Sprintf("ProjectionExec: %s", p.Exprs)
}

func (p ProjectionExec) GetSchema() Schema {
	return p.Schema
}

func (p ProjectionExec) Execute() []RecordBatch {
	input := p.Input.Execute()
	output := make([]RecordBatch, len(input))

	for i, batch := range input {
		columns := make([]ColumnVector, len(p.Exprs))
		for j, expr := range p.Exprs {
			columns[j] = expr.Evaluate(batch)
		}
		output[i] = RecordBatch{p.Schema, columns}
	}
	return output
}

/*
Selection (also known as Filter)

The selection execution plan is the first non-trivial plan,
since it has conditional logic to determine which rows from the input record
batch should be included in the output batches.

For each input batch, the filter expression is evaluated to return a bit vector
containing bits representing the Boolean result of the expression, with one bit
for each row. This bit vector is then used to filter the input columns to
produce new output columns. This is a simple implementation that could be
optimized for cases where the bit vector contains all ones or all zeros to
avoid overhead of copying data to new vectors.
*/
type SelectionExec struct {
	Input PhysicalPlan
	Expr  Expression
}

func (s SelectionExec) GetSchema() Schema {
	return s.Input.GetSchema()
}

func (s SelectionExec) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Input}
}

func (s SelectionExec) Execute() []RecordBatch {
	input := s.Input.Execute()
	output := make([]RecordBatch, len(input))
	for i, batch := range input {
		result := s.Expr.Evaluate(batch)
		schema := batch.Schema
		columnCount := len(schema.Fields())
		filtered := make([]ColumnVector, len(batch.Fields))
		for j := 0; j < columnCount; j++ {
			filtered[j] = filter(batch.Fields[j], result)
		}
		output[i] = RecordBatch{batch.Schema, filtered}
	}
	return output
}

func filter(v ColumnVector, selection ColumnVector) ColumnVector {
	var filteredVector []any
	for i := 0; i < selection.Len(); i++ {
		if selection.GetValue(i).(bool) {
			filteredVector = append(filteredVector, v.GetValue(i))
		}
	}
	return drogo.New(v.DataType(), len(filteredVector), filteredVector)
}
