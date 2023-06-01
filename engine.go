package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
)

// abstraction on top of the arrow FieldVector
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

func (v *LiteralValueVector) GetType() arrow.Type {
	return v.arrowType
}

func (v *LiteralValueVector) GetValue(i int) (interface{}, error) {
	if i < 0 || i >= v.size {
		return nil, fmt.Errorf("index out of bounds %d vecsize: %d", i, v.size)
	}
	return v.value, nil
}

func (v *LiteralValueVector) GetSize() int {
	return v.size
}

// RecordBatch represents a batch of columnar data.
type RecordBatch struct {
	Schema *arrow.Schema
	Fields []ColumnVector
}

func (r *RecordBatch) RowCount() int {
	return r.Fields[0].getSize()
}

func (r *RecordBatch) ColumnCount() int {
	return len(r.Fields)
}

func (r *RecordBatch) Field(i int) ColumnVector {
	return r.Fields[i]
}

type DataSource interface {
	schema() *arrow.Schema
	scan(projection []string) []RecordBatch
}

type LogicalPlan interface {
	schema() *arrow.Schema
	children() []LogicalPlan
	String() string
}

func Format(plan LogicalPlan, indent int) string {
	var sb strings.Builder
	for i := 0; i < indent; i++ {
		sb.WriteRune('\t')
	}
	sb.WriteString(plan.String())
	sb.WriteRune('\n')
	for _, child := range plan.children() {
		sb.WriteString(Format(child, indent+1))
	}
	return sb.String()
}

type LogicalExpr interface {
	toField(input LogicalPlan) arrow.Field
	String() string
}

type Column struct {
	name string
}

func (col *Column) ToField(input LogicalPlan) (*arrow.Field, error) {
	for _, f := range input.schema().Fields() {
		if f.Name == col.name {
			return &f, nil
		}
	}
	return nil, fmt.Errorf("SQLError: No column named '$name'")
}

func (col *Column) String() string {
	return "#" + col.name
}

type LiteralString struct {
	str string
}

func (lit *LiteralString) toField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.str,
		Type:     arrow.BinaryTypes.String,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

type LiteralInt64 struct {
	n int64
}

func (lit *LiteralInt64) toField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     strconv.Itoa(int(lit.n)),
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit *LiteralInt64) String() string {
	return strconv.Itoa(int(lit.n))
}

type BinaryExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (be *BinaryExpr) String() string {
	return fmt.Sprintf("%v %v %v", be.L, be.Op, be.R)
}

type BooleanBinaryExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (be *BooleanBinaryExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: be.Name,
		Type: arrow.FixedWidthTypes.Boolean,
	}
}

func Eq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"eq", "=", l, r}
}

func Neq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"neq", "!=", l, r}
}

func Gt(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gt", ">", l, r}
}
func GtEq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gteq", ">=", l, r}
}
func Lt(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lt", "<", l, r}
}
func LtEq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lteq", "<=", l, r}
}

func And(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"and", "AND", l, r}
}

func Or(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"or", "OR", l, r}
}

type MathExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func Add(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"add", "+", l, r}
}

func Subtract(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"subtract", "-", l, r}
}

func Multiply(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"multiply", "*", l, r}
}

func Divide(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"divide", "/", l, r}
}

func Modulus(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"modulus", "%", l, r}
}

type AggregateExpr struct {
	Name string
	Expr LogicalExpr
}

func (e *AggregateExpr) String() string {
	return fmt.Sprintf("%s(%s)", e.Name, e.Expr.String())
}

func (e *AggregateExpr) toField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: e.Name,
		Type: e.Expr.toField(input).Type,
	}
}

func Sum(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"SUM", input}
}

func Min(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MIN", input}
}

func Max(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MAX", input}
}

func Avg(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"AVG", input}
}

func Count(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"COUNT", input}
}
