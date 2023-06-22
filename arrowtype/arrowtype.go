package arrowtype

import (
	"github.com/apache/arrow/go/v12/arrow"
)

var (
	Int8    = &arrow.Int8Type{}
	Int16   = &arrow.Int16Type{}
	Int32   = &arrow.Int32Type{}
	Int64   = &arrow.Int64Type{}
	Float32 = &arrow.Float32Type{}
	Float64 = &arrow.Float64Type{}
	String  = &arrow.StringType{}
	Boolean = &arrow.BooleanType{}
)
