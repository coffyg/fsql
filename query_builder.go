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

// NEW: Keep track of user-defined fields per alias or table
type fieldsOverride struct {
	aliasOrTable string
	fields       []string
}

type FieldSelectStep struct {
	fieldsOverride
}

type QueryBuilder struct {
	Table string
	Steps []QueryStep
	// A map from "table" or "alias" -> []string of fields.
	// If empty or missing, we call GetSelectFields as before.
	customFields map[string][]string
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

	query := fmt.Sprintf(`UPDATE "%s" SET %s WHERE "%s"."%s" = $%d RETURNING "%s".%s`,
		tableName,
		strings.Join(setClauses, ", "),
		tableName,
		returning,
		counter,
		tableName,
		returning,
	)
	uuidValue, uuidExists := valuesMap[returning]
	if !uuidExists {
		panic(fmt.Sprintf("UUID not found in valuesMap: %v", valuesMap))
	}
	queryValues = append(queryValues, uuidValue)

	return query, queryValues
}

func SelectBase(table string, alias string) *QueryBuilder {
	return &QueryBuilder{
		Table:        table,
		Steps:        []QueryStep{},
		customFields: make(map[string][]string),
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

// NEW: specify custom fields for a table or an alias
func (qb *QueryBuilder) WithFields(aliasOrTable string, fields ...string) *QueryBuilder {
	qb.customFields[aliasOrTable] = fields
	return qb
}

func (qb *QueryBuilder) Build() string {
	var baseWheres []string
	var joinsList []*Join
	var whereConditions []string
	var fields []string
	hasJoins := false

	for _, step := range qb.Steps {
		switch s := step.(type) {
		case WhereStep:
			// We place base Wheres if no joins are used yet. Once we have a join,
			// the logic below will place them after the FROM or subquery.
			if !hasJoins {
				baseWheres = append(baseWheres, s.Condition)
			} else {
				whereConditions = append(whereConditions, s.Condition)
			}
		case JoinStep:
			hasJoins = true
			joinsList = append(joinsList, &s.Join)
		default:
			// no-op
		}
	}

	// Decide the base table or subquery
	var baseTable string
	var baseSelectFields []string
	if qb.customFields[qb.Table] != nil {
		// If user provided custom fields for the base table
		baseSelectFields = buildQuotedFields(qb.Table, "", qb.customFields[qb.Table])
	} else {
		// Default to all fields from the model
		all, _ := GetSelectFields(qb.Table, "")
		baseSelectFields = all
	}

	if len(baseWheres) > 0 {
		// we build a subquery for the base table if there's a where
		subquery := fmt.Sprintf(`SELECT %s FROM "%s" WHERE %s`,
			strings.Join(baseSelectFields, ", "),
			qb.Table,
			strings.Join(baseWheres, " AND "),
		)
		baseTable = fmt.Sprintf(`(%s) AS "%s"`, subquery, qb.Table)
	} else {
		baseTable = fmt.Sprintf(`"%s"`, qb.Table)
	}

	// add base fields to the overall fields
	fields = append(fields, baseSelectFields...)

	// handle joins
	for _, join := range joinsList {
		var joinFields []string
		if qb.customFields[join.TableAlias] != nil {
			// user has custom fields for this alias
			joinFields = buildQuotedFields(join.Table, join.TableAlias, qb.customFields[join.TableAlias])
		} else {
			// no custom fields, select all
			joinFields, _ = GetSelectFields(join.Table, join.TableAlias)
		}
		fields = append(fields, joinFields...)

		tableClause := fmt.Sprintf(`"%s"`, join.Table)
		if join.TableAlias != "" {
			tableClause = fmt.Sprintf(`"%s" AS "%s"`, join.Table, join.TableAlias)
		}
		joinPart := fmt.Sprintf(` %s %s ON %s `, join.JoinType, tableClause, join.OnCondition)
		baseTable += joinPart
	}

	// build the final query
	query := fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(fields, ", "), baseTable)
	if len(whereConditions) > 0 {
		query += " WHERE " + strings.Join(whereConditions, " AND ")
	}
	return query
}

// buildQuotedFields returns a slice of quoted columns, e.g. `"table"."field"` or `"alias"."field" AS "alias.field"`
func buildQuotedFields(tableName, alias string, fields []string) []string {
	var results []string
	tableNameClean := strings.ReplaceAll(tableName, `"`, "")
	aliasClean := strings.ReplaceAll(alias, `"`, "")

	for _, f := range fields {
		fieldQuoted := fmt.Sprintf(`"%s"`, strings.ReplaceAll(f, `"`, `""`))
		if aliasClean != "" {
			results = append(results, fmt.Sprintf(`"%s".%s AS "%s.%s"`, aliasClean, fieldQuoted, aliasClean, f))
		} else {
			results = append(results, fmt.Sprintf(`"%s".%s`, tableNameClean, fieldQuoted))
		}
	}
	return results
}

func GenNewUUID(table string) string {
	return uuid.New().String()
}
