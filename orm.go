package fsql

import (
	"fmt"
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

func GetInsertQuery(tableName string, valuesMap map[string]interface{}, returning string) (string, []interface{}) {
	_, fields := GetInsertFields(tableName)
	defaultValues := GetInsertValues(tableName)

	placeholders := []string{}
	queryValues := []interface{}{}
	counter := 1
	for _, field := range fields {
		if val, ok := valuesMap[field]; ok {
			// If value is provided in valuesMap, use it
			placeholders = append(placeholders, fmt.Sprintf("$%d", counter))
			queryValues = append(queryValues, val)
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
			setClause := fmt.Sprintf(`%s = $%d`, field, counter)

			setClauses = append(setClauses, setClause)
			queryValues = append(queryValues, value)
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
