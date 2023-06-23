package util

func SliceToAny[T any](data []T) []any {
	anyData := make([]any, len(data))
	for i, v := range data {
		anyData[i] = v
	}
	return anyData
}
