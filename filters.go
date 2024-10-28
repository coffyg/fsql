// filters.go
package fsql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lib/pq"
)

type Filter map[string]interface{}
type Sort map[string]string

func constructConditions(t string, filters *Filter, table string) ([]string, []interface{}, error) {
	modelInfo, ok := getModelInfo(table)
	if !ok {
		return nil, nil, fmt.Errorf("table name not initialized: %s", table)
	}

	var conditions []string
	var args []interface{}
	argCounter := 1

	if filters != nil {
		for filterKey, filterValue := range *filters {
			fieldParts := strings.Split(filterKey, "[")
			fieldName := fieldParts[0]
			operator := ""
			if len(fieldParts) > 1 {
				operator = strings.TrimSuffix(fieldParts[1], "]")
			}

			dbField, exists := modelInfo.dbTagMap[fieldName]
			if !exists {
				continue
			}

			conditionStr := getConditionString(operator)
			isArray := operator == "$in" || operator == "$nin"

			shouldLower := strings.HasPrefix(operator, "€")
			if shouldLower {
				condition := fmt.Sprintf(`LOWER("%s".%s) %s`, t, dbField, conditionStr)
				conditions = append(conditions, fmt.Sprintf(condition, argCounter))
				if strVal, ok := filterValue.(string); ok {
					filterValue = strings.ToLower(strVal)
				}
			} else {
				condition := fmt.Sprintf(`"%s".%s %s`, t, dbField, conditionStr)
				conditions = append(conditions, fmt.Sprintf(condition, argCounter))
			}

			if isArray {
				filterValue = pq.Array(filterValue)
			}

			args = append(args, filterValue)
			argCounter++
		}
	}

	return conditions, args, nil
}

func getConditionString(operator string) string {
	switch operator {
	case "$prefix", "€prefix":
		return `LIKE $%d`
	case "$suffix", "€suffix":
		return `LIKE $%d`
	case "$like", "€like":
		return `LIKE $%d`
	case "$gt":
		return `> $%d`
	case "$gte":
		return `>= $%d`
	case "$lt":
		return `< $%d`
	case "$lte":
		return `<= $%d`
	case "$ne":
		return `!= $%d`
	case "$in":
		return `= ANY($%d)`
	case "$nin":
		return `!= ALL($%d)`
	case "$eq", "€eq":
		return `= $%d`
	default:
		return `= $%d`
	}
}

func FilterQuery(baseQuery string, t string, filters *Filter, sort *Sort, table string, perPage int, page int) (string, []interface{}, error) {
	conditions, args, err := constructConditions(t, filters, table)
	if err != nil {
		return "", nil, err
	}

	if len(conditions) > 0 {
		baseQuery += " WHERE " + strings.Join(conditions, " AND ")
	}

	if sort != nil && len(*sort) > 0 {
		sortClauses := []string{}
		modelInfo, _ := getModelInfo(table)

		for field, order := range *sort {
			order = strings.ToUpper(order)
			if order != "ASC" && order != "DESC" {
				return "", nil, fmt.Errorf("invalid sort order: %s", order)
			}
			dbField, exists := modelInfo.dbTagMap[field]
			if exists {
				sortClauses = append(sortClauses, fmt.Sprintf(`"%s".%s %s`, t, dbField, order))
			}
		}

		if len(sortClauses) > 0 {
			baseQuery += " ORDER BY " + strings.Join(sortClauses, ", ")
		}
	}

	limit := perPage
	offset := (page - 1) * perPage
	baseQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	return baseQuery, args, nil
}

var reLimit = regexp.MustCompile(`(?i)\sLIMIT\s+\d+`)
var reOffset = regexp.MustCompile(`(?i)\sOFFSET\s+\d+`)
var reOrderBy = regexp.MustCompile(`(?i)\sORDER\s+BY\s+[^)]+`)

func BuildFilterCount(baseQuery string) string {
	// Remove LIMIT and OFFSET clauses
	baseQuery = reLimit.ReplaceAllString(baseQuery, "")
	baseQuery = reOffset.ReplaceAllString(baseQuery, "")
	baseQuery = strings.TrimSpace(baseQuery)

	// Remove ORDER BY clause
	baseQuery = reOrderBy.ReplaceAllString(baseQuery, "")
	baseQuery = strings.TrimSpace(baseQuery)

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_subquery", baseQuery)
	return countQuery
}

func GetFilterCount(query string, args []interface{}) (int, error) {
	var count int
	err := Db.QueryRow(query, args...).Scan(&count)
	return count, err
}

func FilterQueryCustom(baseQuery string, t string, orderBy string, args []interface{}, perPage int, page int) (string, []interface{}, error) {
	limit := perPage
	offset := (page - 1) * perPage

	baseQuery += fmt.Sprintf(" ORDER BY %s", orderBy)
	baseQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	return baseQuery, args, nil
}

func BuildFilterCountCustom(baseQuery string) string {
	parts := strings.Split(baseQuery, "FROM")
	query := parts[1]

	if strings.Contains(query, "LIMIT") {
		query = strings.Split(query, "LIMIT")[0]
	}

	if strings.Contains(query, "ORDER BY") {
		query = strings.Split(query, "ORDER BY")[0]
	}

	query = strings.TrimSuffix(query, " ")
	query = strings.TrimSuffix(query, ",")

	return "SELECT COUNT(*) FROM " + query

}
