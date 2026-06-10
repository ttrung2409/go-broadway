package pg

import "encoding/json"

// ParseRow converts a Row (map[string]any) into the given type R using JSON
// as the intermediary. Struct tags on R are respected.
func ParseRow[R any](row Row) (R, error) {
	var result R
	b, err := json.Marshal(row)
	if err != nil {
		return result, err
	}
	return result, json.Unmarshal(b, &result)
}
