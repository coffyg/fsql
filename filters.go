// filters.go
package fsql

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/lib/pq"
)

type Filter map[string]interface{}
type Sort map[string]string

// Condition operator constants - improves readability and avoids string comparisons
const (
	opPrefix       = "$prefix"
	opEuroPrefix   = "€prefix"
	opSuffix       = "$suffix"
	opEuroSuffix   = "€suffix"
	opLike         = "$like"
	opEuroLike     = "€like"
	opGreaterThan  = "$gt"
	opGreaterEqual = "$gte"
	opLessThan     = "$lt"
	opLessEqual    = "$lte"
	opNotEqual     = "$ne"
	opIn           = "$in"
	opNotIn        = "$nin"
	opEqual        = "$eq"
	opEuroEqual    = "€eq"
)

// Operator to SQL condition mapping - faster lookup than switch statement
var operatorConditions = map[string]string{
	opPrefix:       `LIKE $%d`,
	opEuroPrefix:   `LIKE $%d`,
	opSuffix:       `LIKE $%d`,
	opEuroSuffix:   `LIKE $%d`,
	opLike:         `LIKE $%d`,
	opEuroLike:     `LIKE $%d`,
	opGreaterThan:  `> $%d`,
	opGreaterEqual: `>= $%d`,
	opLessThan:     `< $%d`,
	opLessEqual:    `<= $%d`,
	opNotEqual:     `!= $%d`,
	opIn:           `= ANY($%d)`,
	opNotIn:        `!= ALL($%d)`,
	opEqual:        `= $%d`,
	opEuroEqual:    `= $%d`,
	"":             `= $%d`, // Default case
}

// Reusable pools for string building operations
var (
	filterConditionBuilderPool = sync.Pool{
		New: func() interface{} {
			sb := strings.Builder{}
			sb.Grow(128) // Pre-allocate a reasonable buffer size
			return &sb
		},
	}
	
	filterConditionsPool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0, 8) // Typical number of conditions
		},
	}
	
	filterArgsPool = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, 8) // Typical number of args
		},
	}
)

func constructConditions(t string, filters *Filter, table string) ([]string, []interface{}, error) {
	modelInfo, ok := getModelInfo(table)
	if !ok {
		return nil, nil, fmt.Errorf("table name not initialized: %s", table)
	}

	// Get pre-allocated slices from pools
	conditions := filterConditionsPool.Get().([]string)
	conditions = conditions[:0] // Reset slice without allocating
	
	args := filterArgsPool.Get().([]interface{})
	args = args[:0] // Reset slice without allocating
	
	// Reusable string builder
	sb := filterConditionBuilderPool.Get().(*strings.Builder)
	defer filterConditionBuilderPool.Put(sb)
	
	// Counter for parameter placeholders
	argCounter := 1

	if filters != nil && len(*filters) > 0 {
		// Pre-allocate enough capacity for the expected number of conditions
		if cap(conditions) < len(*filters) {
			newConditions := make([]string, 0, len(*filters))
			copy(newConditions, conditions)
			conditions = newConditions
		}
		
		if cap(args) < len(*filters) {
			newArgs := make([]interface{}, 0, len(*filters))
			copy(newArgs, args)
			args = newArgs
		}
		
		// Pre-build the quote+table part once
		quotedTable := `"` + t + `"`
		
		for filterKey, filterValue := range *filters {
			// Parse filter key more efficiently
			var fieldName, operator string
			bracketIdx := strings.IndexByte(filterKey, '[')
			if bracketIdx >= 0 {
				fieldName = filterKey[:bracketIdx]
				// Use len-1 to trim the closing bracket too
				operator = filterKey[bracketIdx+1 : len(filterKey)-1]
			} else {
				fieldName = filterKey
				// Default case - empty operator means equals
				operator = ""
			}

			dbField, exists := modelInfo.dbTagMap[fieldName]
			if !exists {
				continue
			}

			// Get condition string from pre-built map
			conditionStr, exists := operatorConditions[operator]
			if !exists {
				// Default to equals if not found
				conditionStr = operatorConditions[""]
			}
			
			// Check if we need to process array values
			isArray := operator == opIn || operator == opNotIn
			
			// Check if we need to use LOWER() for case-insensitive search
			shouldLower := strings.HasPrefix(operator, "€")
			
			// Build the condition string
			sb.Reset()
			
			if shouldLower {
				sb.WriteString("LOWER(")
				sb.WriteString(quotedTable)
				sb.WriteByte('.')
				sb.WriteString(dbField)
				sb.WriteString(") ")
				sb.WriteString(conditionStr)
				
				// Convert string values to lowercase for case-insensitive search
				if strVal, ok := filterValue.(string); ok {
					filterValue = strings.ToLower(strVal)
				}
			} else {
				sb.WriteString(quotedTable)
				sb.WriteByte('.')
				sb.WriteString(dbField)
				sb.WriteByte(' ')
				sb.WriteString(conditionStr)
			}
			
			// Format the parameter placeholder
			condition := fmt.Sprintf(sb.String(), argCounter)
			conditions = append(conditions, condition)

			if isArray {
				filterValue = pq.Array(filterValue)
			}

			args = append(args, filterValue)
			argCounter++
		}
	}

	// Wrap slices in a defer to return them to pool after they're used
	return conditions, args, nil
}

// sortClausePool provides reusable slice for sort clauses to reduce allocations
var sortClausePool = sync.Pool{
	New: func() interface{} {
		return make([]string, 0, 4) // Typical number of sort fields
	},
}

// queryBuilderPool provides reusable string builders for query construction
var queryBuilderPool = sync.Pool{
	New: func() interface{} {
		sb := strings.Builder{}
		sb.Grow(512) // Pre-allocate a reasonable buffer size for queries
		return &sb
	},
}

func FilterQuery(baseQuery string, t string, filters *Filter, sort *Sort, table string, perPage int, page int) (string, []interface{}, error) {
	conditions, args, err := constructConditions(t, filters, table)
	if err != nil {
		return "", nil, err
	}
	
	// Get a string builder from pool
	sb := queryBuilderPool.Get().(*strings.Builder)
	defer queryBuilderPool.Put(sb)
	
	// Start with the base query
	sb.Reset()
	sb.WriteString(baseQuery)

	// Add WHERE clause if there are conditions
	if len(conditions) > 0 {
		sb.WriteString(" WHERE ")
		
		// Join conditions with AND
		for i, condition := range conditions {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(condition)
		}
	}

	// Process sort if present
	if sort != nil && len(*sort) > 0 {
		// Get sort clauses array from pool
		sortClauses := sortClausePool.Get().([]string)
		sortClauses = sortClauses[:0] // Reset without allocating
		
		// Use a local string builder for each sort clause
		clauseSb := filterConditionBuilderPool.Get().(*strings.Builder)
		
		// Get model info once
		modelInfo, _ := getModelInfo(table)
		quotedTable := `"` + t + `"`

		// Ensure capacity
		if cap(sortClauses) < len(*sort) {
			newSortClauses := make([]string, 0, len(*sort))
			copy(newSortClauses, sortClauses)
			sortClauses = newSortClauses
		}

		for field, order := range *sort {
			// Fast check for valid order
			order = strings.ToUpper(order)
			if order != "ASC" && order != "DESC" {
				// Clean up pooled resources
				filterConditionBuilderPool.Put(clauseSb)
				sortClausePool.Put(sortClauses)
				return "", nil, fmt.Errorf("invalid sort order: %s", order)
			}
			
			dbField, exists := modelInfo.dbTagMap[field]
			if exists {
				// Build sort clause efficiently
				clauseSb.Reset()
				clauseSb.WriteString(quotedTable)
				clauseSb.WriteByte('.')
				clauseSb.WriteString(dbField)
				clauseSb.WriteByte(' ')
				clauseSb.WriteString(order)
				
				sortClauses = append(sortClauses, clauseSb.String())
			}
		}
		
		// Return clause builder to pool
		filterConditionBuilderPool.Put(clauseSb)

		// Add ORDER BY if there are sort clauses
		if len(sortClauses) > 0 {
			sb.WriteString(" ORDER BY ")
			
			// Join sort clauses
			for i, clause := range sortClauses {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(clause)
			}
		}
		
		// Return sort clauses to pool
		sortClausePool.Put(sortClauses)
	}

	// Add pagination
	limit := perPage
	offset := (page - 1) * perPage
	sb.WriteString(" LIMIT ")
	sb.WriteString(fmt.Sprint(limit))
	sb.WriteString(" OFFSET ")
	sb.WriteString(fmt.Sprint(offset))

	// If conditions slice was from pool, return it
	if conditions != nil {
		filterConditionsPool.Put(conditions)
	}

	return sb.String(), args, nil
}

// Pre-compiled regular expressions for query parsing
var (
	reLimit = regexp.MustCompile(`(?i)\sLIMIT\s+\d+`)
	reOffset = regexp.MustCompile(`(?i)\sOFFSET\s+\d+`)
	reOrderBy = regexp.MustCompile(`(?i)\sORDER\s+BY\s+[^)]+`)
	
	// Common SQL fragments pre-built for reuse
	selectCountPrefix = "SELECT COUNT(*) FROM ("
	selectCountSuffix = ") AS count_subquery"
	
	// Pool for count query building
	countQueryBuilderPool = sync.Pool{
		New: func() interface{} {
			sb := strings.Builder{}
			sb.Grow(512) // Pre-allocate a reasonable buffer size
			return &sb
		},
	}
)

// BuildFilterCount creates a COUNT query by removing pagination from a base query
func BuildFilterCount(baseQuery string) string {
	// Get string builder from pool
	sb := countQueryBuilderPool.Get().(*strings.Builder)
	defer countQueryBuilderPool.Put(sb)
	sb.Reset()
	
	// Efficiently remove LIMIT, OFFSET, and ORDER BY clauses using indexes
	// Rather than using regex replacements which create new strings
	limitIndex := indexCaseInsensitive(baseQuery, " LIMIT ")
	if limitIndex > 0 {
		baseQuery = baseQuery[:limitIndex]
	}
	
	offsetIndex := indexCaseInsensitive(baseQuery, " OFFSET ")
	if offsetIndex > 0 {
		baseQuery = baseQuery[:offsetIndex]
	}
	
	orderByIndex := indexCaseInsensitive(baseQuery, " ORDER BY ")
	if orderByIndex > 0 {
		baseQuery = baseQuery[:orderByIndex]
	}
	
	// Build count query
	sb.WriteString(selectCountPrefix)
	sb.WriteString(baseQuery)
	sb.WriteString(selectCountSuffix)
	
	return sb.String()
}

// indexCaseInsensitive is a helper function to find case-insensitive substrings
// without the overhead of regular expressions
func indexCaseInsensitive(s, substr string) int {
	s = strings.ToUpper(s)
	substr = strings.ToUpper(substr)
	return strings.Index(s, substr)
}

// GetSortCondition builds a sort condition clause from a Sort map
func GetSortCondition(sort *Sort, table string) (string, error) {
	if sort == nil || len(*sort) == 0 {
		return "", nil
	}
	
	// Get pre-allocated resources from pools
	sortClauses := sortClausePool.Get().([]string)
	sortClauses = sortClauses[:0] // Reset without allocating
	defer sortClausePool.Put(sortClauses)
	
	sb := filterConditionBuilderPool.Get().(*strings.Builder)
	defer filterConditionBuilderPool.Put(sb)
	
	// Get model info once
	modelInfo, _ := getModelInfo(table)
	quotedTable := `"` + table + `"`
	
	// Ensure capacity
	if cap(sortClauses) < len(*sort) {
		newSortClauses := make([]string, 0, len(*sort))
		copy(newSortClauses, sortClauses)
		sortClauses = newSortClauses
	}
	
	for field, order := range *sort {
		// Fast check for valid order
		order = strings.ToUpper(order)
		if order != "ASC" && order != "DESC" {
			return "", fmt.Errorf("invalid sort order: %s", order)
		}
		
		dbField, exists := modelInfo.dbTagMap[field]
		if exists {
			// Build sort clause efficiently
			sb.Reset()
			sb.WriteString(quotedTable)
			sb.WriteByte('.')
			sb.WriteString(dbField)
			sb.WriteByte(' ')
			sb.WriteString(order)
			
			sortClauses = append(sortClauses, sb.String())
		}
	}
	
	// Generate final sort clause
	if len(sortClauses) > 0 {
		sb.Reset()
		sb.WriteString(" ORDER BY ")
		
		for i, clause := range sortClauses {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(clause)
		}
		
		return sb.String(), nil
	}
	
	return "", nil
}

// GetFilterCount executes a count query and returns the result
func GetFilterCount(query string, args []interface{}) (int, error) {
	var count int
	err := Db.QueryRow(query, args...).Scan(&count)
	return count, err
}

// FilterQueryCustom builds a query with custom order by and pagination
func FilterQueryCustom(baseQuery string, t string, orderBy string, args []interface{}, perPage int, page int) (string, []interface{}, error) {
	sb := queryBuilderPool.Get().(*strings.Builder)
	defer queryBuilderPool.Put(sb)
	
	sb.Reset()
	sb.WriteString(baseQuery)
	sb.WriteString(" ORDER BY ")
	sb.WriteString(orderBy)
	
	// Add pagination
	limit := perPage
	offset := (page - 1) * perPage
	sb.WriteString(" LIMIT ")
	sb.WriteString(fmt.Sprint(limit))
	sb.WriteString(" OFFSET ")
	sb.WriteString(fmt.Sprint(offset))
	
	return sb.String(), args, nil
}

// BuildFilterCountCustom creates a count query from a custom base query
func BuildFilterCountCustom(baseQuery string) string {
	// For safety, just wrap the query in a subquery count
	// This avoids parsing issues with complex queries
	sb := countQueryBuilderPool.Get().(*strings.Builder)
	defer countQueryBuilderPool.Put(sb)
	
	sb.Reset()
	sb.WriteString(selectCountPrefix)
	
	// Remove LIMIT, OFFSET, and ORDER BY clauses
	limitIndex := indexCaseInsensitive(baseQuery, " LIMIT ")
	if limitIndex > 0 {
		sb.WriteString(baseQuery[:limitIndex])
	} else {
		offsetIndex := indexCaseInsensitive(baseQuery, " OFFSET ")
		if offsetIndex > 0 {
			sb.WriteString(baseQuery[:offsetIndex])
		} else {
			orderByIndex := indexCaseInsensitive(baseQuery, " ORDER BY ")
			if orderByIndex > 0 {
				sb.WriteString(baseQuery[:orderByIndex])
			} else {
				sb.WriteString(baseQuery)
			}
		}
	}
	
	sb.WriteString(selectCountSuffix)
	return sb.String()
}
