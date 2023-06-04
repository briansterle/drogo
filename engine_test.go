package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogicalPlan(t *testing.T) {
	// data source
	csv := &CsvDataSource{"employees.csv", Schema{}, true, 100}

	// FROM
	scan := Scan{"employee", csv, []string{}}

	// WHERE
	filterExpr := Eq(Column{"state"}, LiteralString{"CO"})

	selection := Selection{scan, filterExpr}

	projection := []LogicalExpr{
		Column{"id"},
		Column{"first_name"},
		Column{"last_name"},
		Column{"state"},
		Column{"salary"},
	}

	plan := Projection{selection, projection}

	actual := Format(plan, 0)
	fmt.Println(actual)

	expected := `Projection: #id, #first_name, #last_name, #state, #salary
	Filter: #state = 'CO'
		Scan: employee; projection=None
`
	assert.Equal(t, expected, actual, "plan should equal")

}

func TestDataFrame(t *testing.T) {
	ctx := &ExecutionContext{}
	plan := ctx.Csv("employees.csv").
		Filter(Eq(Col("state"), Str("OH"))).
		Project([]LogicalExpr{Col("id"), Col("first_name"), Col("last_name"), Col("salary"), Alias{Multiply(Col("salary"), Flt(0.1)), "bonus"}}).
		Filter(GtEq(Col("bonus"), Int(10000))).
		LogicalPlan()
	actual := Format(plan, 0)

	fmt.Println(actual)
	expected := `Filter: #bonus >= 10000
	Projection: #id, #first_name, #last_name, #salary, #salary * 0.1 as bonus
		Filter: #state = 'OH'
			Scan: employees.csv; projection=None
`

	assert.Equal(t, expected, actual, "plan should equal")
}
