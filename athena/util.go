package athena

import (
	"fmt"
)

const invalidLimit = constError("limit must be non-negative")
const emptyTable = constError("table must not be an empty string")

// NRows returns a query string for selecting at most N rows (where 0 <= N <= limit)
// from table.
func NRows(table string, limit int) (string, error) {
	if table == "" {
		return "", emptyTable
	}

	if limit < 0 {
		return "", invalidLimit
	}

	return fmt.Sprintf("SELECT * FROM %s LIMIT %d", table, limit), nil
}
