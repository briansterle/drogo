package engine

import (
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
)

type DataFrame interface {
	Project(expr []LogicalExpr) DataFrame
	Filter(expr LogicalExpr) DataFrame
	Aggregate(groupBy []LogicalExpr, aggregateExpr []AggregateExpr) DataFrame
	Schema() Schema
	LogicalPlan() LogicalPlan
}

type DataFrameImpl struct {
	plan LogicalPlan
}

func (df *DataFrameImpl) Project(expr []LogicalExpr) DataFrame {
	return &DataFrameImpl{Projection{df.plan, expr}}
}

func (df *DataFrameImpl) Filter(expr LogicalExpr) DataFrame {
	return &DataFrameImpl{Selection{df.plan, expr}}
}

func (df *DataFrameImpl) Aggregate(groupBy []LogicalExpr, aggregateExpr []AggregateExpr) DataFrame {
	return &DataFrameImpl{Aggregate{df.plan, groupBy, aggregateExpr}}
}

func (df *DataFrameImpl) Schema() Schema {
	return df.plan.Schema()
}

func (df *DataFrameImpl) LogicalPlan() LogicalPlan {
	return df.plan
}

type ExecutionContext struct{}

func (ec *ExecutionContext) Csv(filename string) DataFrame {
	return &DataFrameImpl{Scan{filename, &CsvDataSource{Filename: filename}, []string{}}}
}

func Col(name string) Column {
	return Column{name}
}
func Str(val string) LiteralString {
	return LiteralString{val}
}
func Int(val int64) LiteralInt64 {
	return LiteralInt64{val}
}
func Flt(val float64) LiteralFloat64 {
	return LiteralFloat64{val}
}

type Alias struct {
	Expr  LogicalExpr
	Alias string
}

func (expr Alias) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: expr.Alias,
		Type: expr.Expr.ToField(input).Type,
	}
}

func (expr Alias) String() string {
	return fmt.Sprintf("%s as %s", expr.Expr.String(), expr.Alias)
}
