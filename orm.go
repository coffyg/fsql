package fsql

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"
)

type Join struct {
	Table       string
	TableAlias  string
	JoinType    string
	OnCondition string
}

type QueryStep interface{}

type WhereStep struct {
	Condition string
}

type JoinStep struct {
	Join
}

type QueryBuilder struct {
	Table string
	Steps []QueryStep
}

// isJSONBType checks if a value should be cast to JSONB in SQL queries.
// This is needed for PgBouncer transaction pooling mode which doesn't support prepared statements.
func isJSONBType(val interface{}) bool {
	if val == nil {
		return false
	}

	// Check if it's a nil pointer
	reflectVal := reflect.ValueOf(val)
	if reflectVal.Kind() == reflect.Ptr && reflectVal.IsNil() {
		return false
	}

	// Additional check: see if it's a map or has "LocalizedText" or "Dictionary" in the type name
	// This helps identify octypes.LocalizedText and octypes.IntDictionary
	typeName := reflect.TypeOf(val).String()
	if strings.Contains(typeName, "LocalizedText") || strings.Contains(typeName, "Dictionary") {
		return true
	}

	// Check if the type implements driver.Valuer
	valuer, ok := val.(driver.Valuer)
	if !ok {
		return false
	}

	// Call Value() to see if it returns []byte (JSON)
	driverVal, err := valuer.Value()
	if err != nil || driverVal == nil {
		return false
	}

	// Check if the result is []byte (which is what json.Marshal returns)
	_, isByte := driverVal.([]byte)
	if !isByte {
		return false
	}

	// Check if the underlying value is a map (common for JSON types)
	if reflectVal.Kind() == reflect.Ptr {
		reflectVal = reflectVal.Elem()
	}
	if reflectVal.Kind() == reflect.Map {
		return true
	}

	return false
}

func GetInsertQuery(tableName string, valuesMap map[string]interface{}, returning string) (string, []interface{}) {
	_, fields := GetInsertFields(tableName)
	defaultValues := GetInsertValues(tableName)

	placeholders := []string{}
	queryValues := []interface{}{}
	counter := 1
	for _, field := range fields {
		if val, ok := valuesMap[field]; ok {
			// If value is provided in valuesMap, use it
			// Add ::jsonb cast for JSONB types (needed for PgBouncer transaction pooling)
			if isJSONBType(val) {
				placeholders = append(placeholders, fmt.Sprintf("$%d::jsonb", counter))
				// Get JSON from Value() method to preserve correct field names
				if valuer, ok := val.(driver.Valuer); ok {
					driverVal, err := valuer.Value()
					if err == nil && driverVal != nil {
						if jsonBytes, ok := driverVal.([]byte); ok {
							queryValues = append(queryValues, string(jsonBytes))
						} else {
							queryValues = append(queryValues, val)
						}
					} else {
						queryValues = append(queryValues, val)
					}
				} else {
					queryValues = append(queryValues, val)
				}
			} else {
				placeholders = append(placeholders, fmt.Sprintf("$%d", counter))
				queryValues = append(queryValues, val)
			}
			counter++
		} else if defVal, ok := defaultValues[field]; ok {
			// Else use the default value from tags
			if defVal == "NOW()" || defVal == "NULL" || defVal == "true" || defVal == "false" || defVal == "DEFAULT" {
				placeholders = append(placeholders, defVal)
			} else {
				placeholders = append(placeholders, fmt.Sprintf("$%d", counter))
				queryValues = append(queryValues, defVal)
				counter++
			}
		}
	}

	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, tableName, strings.Join(fields, ","), strings.Join(placeholders, ","))
	if len(returning) > 0 {
		query += fmt.Sprintf(` RETURNING "%s".%s`, tableName, returning)
	}
	return query, queryValues
}

func GetUpdateQuery(tableName string, valuesMap map[string]interface{}, returning string) (string, []interface{}) {
	_, fields := GetUpdateFields(tableName)
	setClauses := []string{}
	queryValues := []interface{}{}
	counter := 1

	for _, field := range fields {
		if value, exists := valuesMap[field]; exists {
			// Add ::jsonb cast for JSONB types (needed for PgBouncer transaction pooling)
			var setClause string
			if isJSONBType(value) {
				setClause = fmt.Sprintf(`%s = $%d::jsonb`, field, counter)
				// Get JSON from Value() method to preserve correct field names
				if valuer, ok := value.(driver.Valuer); ok {
					driverVal, err := valuer.Value()
					if err == nil && driverVal != nil {
						if jsonBytes, ok := driverVal.([]byte); ok {
							queryValues = append(queryValues, string(jsonBytes))
						} else {
							queryValues = append(queryValues, value)
						}
					} else {
						queryValues = append(queryValues, value)
					}
				} else {
					queryValues = append(queryValues, value)
				}
			} else {
				setClause = fmt.Sprintf(`%s = $%d`, field, counter)
				queryValues = append(queryValues, value)
			}

			setClauses = append(setClauses, setClause)
			counter++
		}
	}

	query := fmt.Sprintf(`UPDATE "%s" SET %s WHERE "%s"."%s" = $%d RETURNING "%s".%s`, tableName, strings.Join(setClauses, ", "), tableName, returning, counter, tableName, returning)
	uuidValue, uuidExists := valuesMap[returning]
	if !uuidExists {
		panic(fmt.Sprintf("UUID not found in valuesMap: %v", valuesMap))
	}
	queryValues = append(queryValues, uuidValue)

	return query, queryValues
}

func SelectBase(table string, alias string) *QueryBuilder {
	return &QueryBuilder{
		Table: table,
		Steps: []QueryStep{},
	}
}

func (qb *QueryBuilder) Where(condition string) *QueryBuilder {
	qb.Steps = append(qb.Steps, WhereStep{Condition: condition})
	return qb
}

func (qb *QueryBuilder) Join(table string, alias string, on string) *QueryBuilder {
	qb.Steps = append(qb.Steps, JoinStep{Join{
		Table:       table,
		TableAlias:  alias,
		JoinType:    "JOIN",
		OnCondition: on,
	}})
	return qb
}

func (qb *QueryBuilder) Left(table string, alias string, on string) *QueryBuilder {
	qb.Steps = append(qb.Steps, JoinStep{Join{
		Table:       table,
		TableAlias:  alias,
		JoinType:    "LEFT JOIN",
		OnCondition: on,
	}})
	return qb
}

func (qb *QueryBuilder) Build() string {
	var baseWheres []string
	var joinsList []*Join
	var whereConditions []string
	var fields []string
	var baseFields []string
	hasJoins := false

	// Collect fields from base table
	baseFields, _ = GetSelectFields(qb.Table, "")
	fields = append(fields, baseFields...)

	for _, step := range qb.Steps {
		switch s := step.(type) {
		case WhereStep:
			if !hasJoins {
				baseWheres = append(baseWheres, s.Condition)
			} else {
				whereConditions = append(whereConditions, s.Condition)
			}
		case JoinStep:
			hasJoins = true
			// Collect fields from join table
			joinFields, _ := GetSelectFields(s.Join.Table, s.Join.TableAlias)
			fields = append(fields, joinFields...)
			// Add join to joinsList
			joinsList = append(joinsList, &s.Join)
		default:
			// Handle other steps if necessary
		}
	}

	// Build base table without using SELECT *
	var baseTable string
	if len(baseWheres) > 0 {
		baseTable = fmt.Sprintf(`(SELECT %s FROM "%s" WHERE %s) AS "%s"`, strings.Join(baseFields, ", "), qb.Table, strings.Join(baseWheres, " AND "), qb.Table)
	} else {
		baseTable = qb.Table
	}

	// Build joins
	var joins []string
	for _, join := range joinsList {
		table := join.Table
		if join.TableAlias != "" {
			table = fmt.Sprintf(`"%s" AS %s`, join.Table, join.TableAlias)
		}
		joins = append(joins, fmt.Sprintf(` %s %s ON %s `, join.JoinType, table, join.OnCondition))
	}

	// Build query
	query := fmt.Sprintf(`SELECT %s FROM "%s" `, strings.Join(fields, ", "), baseTable)

	if len(joins) > 0 {
		query += " " + strings.Join(joins, " ")
	}

	if len(whereConditions) > 0 {
		query += " WHERE " + strings.Join(whereConditions, " AND ")
	}

	return query
}

func GenNewUUID(table string) string {
	return uuid.New().String()
}
