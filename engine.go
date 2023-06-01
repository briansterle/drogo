package engine

import (
	"fmt"
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
