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

type QueryBuilder struct {
	Table string
	Joins []Join
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
		Joins: []Join{},
	}
}

func (qb *QueryBuilder) Left(table string, alias string, on string) *QueryBuilder {
	qb.Joins = append(qb.Joins, Join{
		Table:       table,
		TableAlias:  alias,
		JoinType:    "LEFT JOIN",
		OnCondition: on,
	})
	return qb
}

func (qb *QueryBuilder) Join(table string, alias string, on string) *QueryBuilder {
	qb.Joins = append(qb.Joins, Join{
		Table:       table,
		TableAlias:  alias,
		JoinType:    "JOIN",
		OnCondition: on,
	})
	return qb
}

func (qb *QueryBuilder) Build() string {
	fieldsArray, _ := GetSelectFields(qb.Table, "")
	fields := strings.Join(fieldsArray, ",")

	for _, join := range qb.Joins {
		fieldsArray, _ := GetSelectFields(join.Table, join.TableAlias)
		fields += ", " + strings.Join(fieldsArray, ",")
	}

	var joins []string
	for _, join := range qb.Joins {
		table := join.Table
		if join.TableAlias != "" {
			table = fmt.Sprintf(`"%s" AS %s`, join.Table, join.TableAlias)
		}
		joins = append(joins, fmt.Sprintf(` %s %s ON %s `, join.JoinType, table, join.OnCondition))
	}

	return fmt.Sprintf(`SELECT %s FROM "%s" %s`, fields, qb.Table, strings.Join(joins, " "))
}

func GenNewUUID(table string) string {
	return uuid.New().String()
}
