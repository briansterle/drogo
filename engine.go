package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
)

// struct embed arrow.Schema to add new methods + convenience
type Schema struct {
	*arrow.Schema
}

// abstraction on top of the arrow FieldVector
type ColumnVector interface {
	DataType() arrow.DataType
	GetValue(i int) interface{}
	Len() int
}

type LiteralValueVector struct {
	arrowType arrow.DataType
	value     interface{}
	size      int
}

func (v LiteralValueVector) DataType() arrow.DataType {
	return v.arrowType
}

func (v LiteralValueVector) GetValue(i int) interface{} {
	if i < 0 || i >= v.size {
		panic(fmt.Sprintf("index out of bounds %d vecsize: %d", i, v.size))
	}
	return v.value
}

func (v LiteralValueVector) Len() int {
	return v.size
}

// RecordBatch represents a batch of columnar data.
type RecordBatch struct {
	Schema Schema
	Fields []ColumnVector
}

func (r *RecordBatch) RowCount() int {
	return r.Fields[0].Len()
}

func (r *RecordBatch) ColumnCount() int {
	return len(r.Fields)
}

func (r *RecordBatch) Field(i int) ColumnVector {
	return r.Fields[i]
}

type DataSource interface {
	GetSchema() Schema
	Scan(projection []string) []RecordBatch
}

type LogicalPlan interface {
	Schema() Schema
	Children() []LogicalPlan
	String() string
}

func Format(plan LogicalPlan, indent int) string {
	var sb strings.Builder
	for i := 0; i < indent; i++ {
		sb.WriteRune('\t')
	}
	sb.WriteString(plan.String())
	sb.WriteRune('\n')
	for _, child := range plan.Children() {
		sb.WriteString(Format(child, indent+1))
	}
	return sb.String()
}

type LogicalExpr interface {
	ToField(input LogicalPlan) arrow.Field
	String() string
}

type Column struct {
	name string
}

func (col Column) ToField(input LogicalPlan) arrow.Field {
	for _, f := range input.Schema().Fields() {
		if f.Name == col.name {
			return f
		}
	}
	panic("SQLError: No column named '$name'")

}

func (col Column) String() string {
	return "#" + col.name
}

type LiteralString struct {
	Str string
}

func (lit LiteralString) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.Str,
		Type:     arrow.BinaryTypes.String,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralString) String() string {
	return fmt.Sprintf("'%s'", lit.Str)
}

type LiteralInt64 struct {
	n int64
}

func (lit LiteralInt64) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.String(),
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralInt64) String() string {
	return strconv.Itoa(int(lit.n))
}

type LiteralFloat64 struct {
	n float64
}

func (lit LiteralFloat64) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.String(),
		Type:     arrow.PrimitiveTypes.Float64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralFloat64) String() string {
	return strconv.FormatFloat(lit.n, 'f', -1, 64)
}

type BinaryExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (be BinaryExpr) String() string {
	return fmt.Sprintf("%v %v %v", be.L, be.Op, be.R)
}

type BooleanBinaryExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (be BooleanBinaryExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: be.Name,
		Type: arrow.FixedWidthTypes.Boolean,
	}
}

func (be BooleanBinaryExpr) String() string {
	return be.L.String() + " " + be.Op + " " + be.R.String()
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

func (m MathExpr) String() string {
	return fmt.Sprintf("%v %v %v", m.L, m.Op, m.R)
}

func (m MathExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: m.Name,
		Type: arrow.PrimitiveTypes.Float64,
	}
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
		Type: e.Expr.ToField(input).Type,
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

type Scan struct {
	Path       string
	Source     DataSource
	Projection []string
}

func (s Schema) Select(projection []string) Schema {
	fields := make([]arrow.Field, 0)
	for _, columnName := range projection {
		field, ok := s.FieldsByName(columnName)
		if ok {
			fields = append(fields, field...)
		}
	}
	new := arrow.NewSchema(fields, nil)
	return Schema{new}
}

func (s Scan) Schema() Schema {
	schema := s.Source.GetSchema()
	if len(s.Projection) == 0 {
		return schema
	} else {
		return schema.Select(s.Projection)
	}
}

func (s Scan) Children() []LogicalPlan {
	return []LogicalPlan{}
}

func (s Scan) String() string {
	if len(s.Projection) == 0 {
		return fmt.Sprintf("Scan: %s; projection=None", s.Path)
	}
	return fmt.Sprintf("Scan: %s; projection=%v", s.Path, s.Projection)
}

type Projection struct {
	Input LogicalPlan
	Expr  []LogicalExpr
}

func (p Projection) Schema() Schema {
	fields := []arrow.Field{}
	for _, e := range p.Expr {
		fields = append(fields, e.ToField(p.Input))
	}
	return Schema{arrow.NewSchema(fields, nil)}
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Input}
}

func (p Projection) String() string {
	strs := []string{}
	for _, e := range p.Expr {
		strs = append(strs, e.String())
	}
	s := strings.Join(strs, ", ")
	return fmt.Sprintf("Projection: %s", s)
}

type Selection struct {
	Input LogicalPlan
	Expr  LogicalExpr
}

func (s Selection) Schema() Schema {
	return s.Input.Schema()
}

func (s Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.Input}
}

func (s Selection) String() string {
	return fmt.Sprintf("Filter: %s", s.Expr.String())
}

type Aggregate struct {
	Input         LogicalPlan
	GroupExpr     []LogicalExpr
	AggregateExpr []AggregateExpr
}

func (a Aggregate) Schema() Schema {
	fields := []arrow.Field{}
	for _, e := range a.GroupExpr {
		fields = append(fields, e.ToField(a.Input))
	}
	for _, e := range a.AggregateExpr {
		fields = append(fields, e.toField(a.Input))
	}
	return Schema{arrow.NewSchema(fields, nil)}
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Input}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%s, aggregateExpr=%s", a.GroupExpr, a.AggregateExpr)
}
