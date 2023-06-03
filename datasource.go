package engine

type CsvDataSource struct {
	Filename   string
	Schema     Schema
	hasHeaders bool
	batchSize  int
}

func (ds *CsvDataSource) GetSchema() Schema {
	return ds.Schema
}

func (ds *CsvDataSource) Scan(projection []string) []RecordBatch {
	return []RecordBatch{{ds.Schema, []ColumnVector{}}}
}
