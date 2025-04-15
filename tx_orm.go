// tx_orm.go
package fsql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Custom query step types for transaction query builder
type orderByStep struct {
	Clause string
}

type limitStep struct {
	Limit int64
}

type offsetStep struct {
	Offset int64
}

// InsertWithTx inserts a record within a transaction
func InsertWithTx(tx *Tx, object interface{}, tableName string) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	
	// Get model tag cache for the object
	objectType := reflect.TypeOf(object)
	if objectType.Kind() == reflect.Ptr {
		objectType = objectType.Elem()
	}
	
	tagCache, err := getModelTagCache(objectType)
	if err != nil {
		return fmt.Errorf("failed to get model tag cache: %w", err)
	}
	
	// Build query for insertion
	var columns []string
	var placeholders []string
	var values []interface{}
	
	// Process insert fields
	for i, field := range tagCache.Fields {
		// Only include insert fields
		if strings.Contains(field.Mode, "i") {
			columns = append(columns, field.DbName)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(values)+1))
			
			// Get field value
			val := reflect.ValueOf(object)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			fieldVal := val.Field(i).Interface()
			
			// Apply any value transformation
			if field.InsertValue != "" {
				fieldVal = field.InsertValue
			}
			
			values = append(values, fieldVal)
		}
	}
	
	if len(columns) == 0 {
		return fmt.Errorf("no fields marked for insertion")
	}
	
	// Build and execute query
	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`,
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))
	
	_, err = tx.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}
	
	return nil
}

// UpdateWithTx updates a record within a transaction
func UpdateWithTx(tx *Tx, object interface{}, tableName, whereClause string, whereArgs ...interface{}) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	
	// Get model tag cache for the object
	objectType := reflect.TypeOf(object)
	if objectType.Kind() == reflect.Ptr {
		objectType = objectType.Elem()
	}
	
	tagCache, err := getModelTagCache(objectType)
	if err != nil {
		return fmt.Errorf("failed to get model tag cache: %w", err)
	}
	
	// Build query for update
	var setClause []string
	var values []interface{}
	
	// Process update fields
	for i, field := range tagCache.Fields {
		// Only include update fields
		if strings.Contains(field.Mode, "u") {
			setClause = append(setClause, fmt.Sprintf("%s = $%d", field.DbName, len(values)+1))
			
			// Get field value
			val := reflect.ValueOf(object)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			fieldVal := val.Field(i).Interface()
			
			values = append(values, fieldVal)
		}
	}
	
	if len(setClause) == 0 {
		return fmt.Errorf("no fields marked for update")
	}
	
	// Add where args to values
	paramOffset := len(values)
	for i, arg := range whereArgs {
		// Replace placeholders in where clause
		whereClause = strings.Replace(whereClause, 
			fmt.Sprintf("$%d", i+1), 
			fmt.Sprintf("$%d", paramOffset+i+1), 1)
		values = append(values, arg)
	}
	
	// Build and execute query
	query := fmt.Sprintf(`UPDATE "%s" SET %s WHERE %s`,
		tableName,
		strings.Join(setClause, ", "),
		whereClause)
	
	_, err = tx.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}
	
	return nil
}

// DeleteWithTx deletes a record within a transaction
func DeleteWithTx(tx *Tx, tableName, whereClause string, whereArgs ...interface{}) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	
	// Build and execute query
	query := fmt.Sprintf(`DELETE FROM "%s" WHERE %s`, tableName, whereClause)
	
	_, err := tx.Exec(query, whereArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute delete: %w", err)
	}
	
	return nil
}

// SelectWithTx selects records within a transaction
func SelectWithTx(tx *Tx, dest interface{}, query string, args ...interface{}) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	
	return tx.Select(dest, query, args...)
}

// GetWithTx gets a single record within a transaction
func GetWithTx(tx *Tx, dest interface{}, query string, args ...interface{}) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	
	return tx.Get(dest, query, args...)
}

// QueryBuilderWithTx extends query builder with transaction support
type QueryBuilderWithTx struct {
	tx *Tx
	qb *QueryBuilder
}

// NewQueryBuilderWithTx creates a new query builder with transaction support
func NewQueryBuilderWithTx(tx *Tx, tableName string) *QueryBuilderWithTx {
	return &QueryBuilderWithTx{
		tx: tx,
		qb: SelectBase(tableName, ""),
	}
}

// Where adds a where clause
func (qb *QueryBuilderWithTx) Where(condition string, args ...interface{}) *QueryBuilderWithTx {
	qb.qb.Where(condition)
	return qb
}

// Join adds a join clause
func (qb *QueryBuilderWithTx) Join(table string, alias string, on string) *QueryBuilderWithTx {
	qb.qb.Join(table, alias, on)
	return qb
}

// Left adds a left join clause
func (qb *QueryBuilderWithTx) Left(table string, alias string, on string) *QueryBuilderWithTx {
	qb.qb.Left(table, alias, on)
	return qb
}

// OrderBy adds an order by clause
func (qb *QueryBuilderWithTx) OrderBy(clause string) *QueryBuilderWithTx {
	// Add OrderBy as a custom step
	qb.qb.Steps = append(qb.qb.Steps, orderByStep{clause})
	return qb
}

// Limit adds a limit clause
func (qb *QueryBuilderWithTx) Limit(limit int64) *QueryBuilderWithTx {
	// Add Limit as a custom step
	qb.qb.Steps = append(qb.qb.Steps, limitStep{limit})
	return qb
}

// Offset adds an offset clause
func (qb *QueryBuilderWithTx) Offset(offset int64) *QueryBuilderWithTx {
	// Add Offset as a custom step
	qb.qb.Steps = append(qb.qb.Steps, offsetStep{offset})
	return qb
}

// Select executes a select query within the transaction
func (qb *QueryBuilderWithTx) Select(dest interface{}) error {
	// Build query from query builder
	query := qb.qb.Build()
	return qb.tx.Select(dest, query)
}

// Get executes a query to get a single record within the transaction
func (qb *QueryBuilderWithTx) Get(dest interface{}) error {
	// Build query from query builder
	query := qb.qb.Build()
	return qb.tx.Get(dest, query)
}

// Common transaction patterns

// InsertWithTxRetry inserts a record with transaction retry
func InsertWithTxRetry(ctx context.Context, object interface{}, tableName string) error {
	return WithTxRetry(ctx, func(tx *Tx) error {
		return InsertWithTx(tx, object, tableName)
	})
}

// UpdateWithTxRetry updates a record with transaction retry
func UpdateWithTxRetry(ctx context.Context, object interface{}, tableName, whereClause string, whereArgs ...interface{}) error {
	return WithTxRetry(ctx, func(tx *Tx) error {
		return UpdateWithTx(tx, object, tableName, whereClause, whereArgs...)
	})
}

// DeleteWithTxRetry deletes a record with transaction retry
func DeleteWithTxRetry(ctx context.Context, tableName, whereClause string, whereArgs ...interface{}) error {
	return WithTxRetry(ctx, func(tx *Tx) error {
		return DeleteWithTx(tx, tableName, whereClause, whereArgs...)
	})
}

// InsertBatchWithTx inserts multiple records within a transaction
func InsertBatchWithTx(tx *Tx, objects interface{}, tableName string) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	
	// Get the slice value and check if it's empty
	val := reflect.ValueOf(objects)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("objects must be a slice")
	}
	if val.Len() == 0 {
		return nil // Nothing to insert
	}
	
	// Get the first object (type already determined during initialization)
	
	// Create a batch executor with optimal batch size (100) determined by benchmarks
	batchInsert := NewBatchInsertExecutor(tableName, BatchSize)
	
	// Add each object to the batch
	for i := 0; i < val.Len(); i++ {
		obj := val.Index(i).Interface()
		err := batchInsert.Add(obj)
		if err != nil {
			return fmt.Errorf("failed to add object to batch: %w", err)
		}
	}
	
	// Flush the batch using the transaction
	err := batchInsert.FlushWithTx(tx)
	if err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}
	
	return nil
}