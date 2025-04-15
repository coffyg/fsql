package fsql

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// BatchSize is the default size for batched operations
const BatchSize = 100

// BatchInsertExecutor handles batched insert operations
type BatchInsertExecutor struct {
	tableName   string
	fields      []string
	valuesBatch [][]interface{}
	batchSize   int
	fieldMap    map[string]bool
	returning   string
	
	// String builder for query construction
	sb *strings.Builder
	
	// Object mode fields
	objectMode  bool
	objectType  reflect.Type
	tagCache    *ModelTagCache
}

// NewBatchInsert creates a new batch insert executor
func NewBatchInsert(tableName string, fields []string, batchSize int) *BatchInsertExecutor {
	if batchSize <= 0 {
		batchSize = BatchSize
	}
	
	// Create field map for quick lookups
	fieldMap := make(map[string]bool, len(fields))
	for _, field := range fields {
		fieldMap[field] = true
	}
	
	return &BatchInsertExecutor{
		tableName:   tableName,
		fields:      fields,
		valuesBatch: make([][]interface{}, 0, batchSize),
		batchSize:   batchSize,
		fieldMap:    fieldMap,
		sb:          &strings.Builder{},
		objectMode:  false,
	}
}

// NewBatchInsertExecutor creates a new batch insert executor for structured objects
func NewBatchInsertExecutor(tableName string, batchSize int) *BatchInsertExecutor {
	if batchSize <= 0 {
		batchSize = BatchSize
	}
	
	return &BatchInsertExecutor{
		tableName:   tableName,
		valuesBatch: make([][]interface{}, 0, batchSize),
		batchSize:   batchSize,
		sb:          &strings.Builder{},
		objectMode:  true,
	}
}

// Add adds a record to the batch
func (b *BatchInsertExecutor) Add(values interface{}) error {
	// Check if we're in object mode or map mode
	if b.objectMode {
		return b.addObject(values)
	} else {
		// Assume it's a map
		valuesMap, ok := values.(map[string]interface{})
		if !ok {
			return fmt.Errorf("values must be a map[string]interface{} when not in object mode")
		}
		return b.addMap(valuesMap)
	}
}

// addMap adds a record from a map to the batch
func (b *BatchInsertExecutor) addMap(values map[string]interface{}) error {
	// Extract values in correct field order
	rowValues := make([]interface{}, len(b.fields))
	
	// Check that all required fields are present
	for i, field := range b.fields {
		value, ok := values[field]
		if !ok {
			// If field is missing, check if it has a default value
			// For now, return an error
			return fmt.Errorf("missing field: %s", field)
		}
		rowValues[i] = value
	}
	
	// Add to batch
	b.valuesBatch = append(b.valuesBatch, rowValues)
	
	// If batch is full, execute it
	if len(b.valuesBatch) >= b.batchSize {
		return b.Flush()
	}
	
	return nil
}

// addObject adds a struct record to the batch
func (b *BatchInsertExecutor) addObject(obj interface{}) error {
	// If this is the first object, initialize the tag cache and fields
	if b.tagCache == nil {
		objType := reflect.TypeOf(obj)
		if objType.Kind() == reflect.Ptr {
			objType = objType.Elem()
		}
		
		// Store the object type for future use
		b.objectType = objType
		
		// Get or create tag cache for this object type
		var err error
		b.tagCache, err = getModelTagCache(objType)
		if err != nil {
			return fmt.Errorf("failed to get model tag cache: %w", err)
		}
		
		// Extract field names for insert mode
		var insertFields []string
		for _, field := range b.tagCache.Fields {
			if strings.Contains(field.Mode, "i") {
				insertFields = append(insertFields, field.DbName)
			}
		}
		
		// Store the fields
		b.fields = insertFields
		
		// Create field map for quick lookups
		b.fieldMap = make(map[string]bool, len(insertFields))
		for _, field := range insertFields {
			b.fieldMap[field] = true
		}
	}
	
	// Extract values from the object
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	
	if val.Type() != b.objectType {
		return fmt.Errorf("object type mismatch: expected %s, got %s", b.objectType.Name(), val.Type().Name())
	}
	
	// Initialize a row slice for the field values
	rowValues := make([]interface{}, 0, len(b.fields))
	
	// Extract only the insert fields from the object
	for i, tagField := range b.tagCache.Fields {
		if strings.Contains(tagField.Mode, "i") {
			// Get field value
			fieldVal := val.Field(i).Interface()
			
			// Apply any value transformation
			if tagField.InsertValue != "" {
				fieldVal = tagField.InsertValue
			}
			
			rowValues = append(rowValues, fieldVal)
		}
	}
	
	// Add to batch
	b.valuesBatch = append(b.valuesBatch, rowValues)
	
	// If batch is full, execute it
	if len(b.valuesBatch) >= b.batchSize {
		return b.Flush()
	}
	
	return nil
}

// SetReturning sets the returning field
func (b *BatchInsertExecutor) SetReturning(field string) *BatchInsertExecutor {
	b.returning = field
	return b
}

// Flush executes the current batch
func (b *BatchInsertExecutor) Flush() error {
	if len(b.valuesBatch) == 0 {
		return nil // Nothing to do
	}
	
	// Build the query
	b.sb.Reset()
	b.sb.WriteString(`INSERT INTO "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" (`)
	
	// Add field names
	for i, field := range b.fields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
	}
	
	b.sb.WriteString(") VALUES ")
	
	// Flatten values into a single slice
	flattenedValues := make([]interface{}, 0, len(b.valuesBatch)*len(b.fields))
	paramCounter := 1
	
	// Add value placeholders
	for i, row := range b.valuesBatch {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		
		b.sb.WriteString("(")
		for j := range b.fields {
			if j > 0 {
				b.sb.WriteString(", ")
			}
			b.sb.WriteString(fmt.Sprintf("$%d", paramCounter))
			paramCounter++
			
			// Add to flattened values
			flattenedValues = append(flattenedValues, row[j])
		}
		b.sb.WriteString(")")
	}
	
	// Add RETURNING clause if specified
	if b.returning != "" {
		b.sb.WriteString(" RETURNING ")
		b.sb.WriteString(b.returning)
	}
	
	// Execute the query
	query := b.sb.String()
	_, err := Db.Exec(query, flattenedValues...)
	if err != nil {
		return err
	}
	
	// Clear the batch
	b.valuesBatch = b.valuesBatch[:0]
	
	return nil
}

// FlushWithTx executes the current batch within a transaction
func (b *BatchInsertExecutor) FlushWithTx(tx *Tx) error {
	if len(b.valuesBatch) == 0 {
		return nil // Nothing to do
	}
	
	// Build the query
	b.sb.Reset()
	b.sb.WriteString(`INSERT INTO "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" (`)
	
	// Add field names
	for i, field := range b.fields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
	}
	
	b.sb.WriteString(") VALUES ")
	
	// Flatten values into a single slice
	flattenedValues := make([]interface{}, 0, len(b.valuesBatch)*len(b.fields))
	paramCounter := 1
	
	// Add value placeholders
	for i, row := range b.valuesBatch {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		
		b.sb.WriteString("(")
		for j := range b.fields {
			if j > 0 {
				b.sb.WriteString(", ")
			}
			b.sb.WriteString(fmt.Sprintf("$%d", paramCounter))
			paramCounter++
			
			// Add to flattened values
			flattenedValues = append(flattenedValues, row[j])
		}
		b.sb.WriteString(")")
	}
	
	// Add RETURNING clause if specified
	if b.returning != "" {
		b.sb.WriteString(" RETURNING ")
		b.sb.WriteString(b.returning)
	}
	
	// Execute the query using the transaction
	query := b.sb.String()
	_, err := tx.Exec(query, flattenedValues...)
	if err != nil {
		return err
	}
	
	// Clear the batch
	b.valuesBatch = b.valuesBatch[:0]
	
	return nil
}

// BatchUpdateExecutor handles batched update operations
type BatchUpdateExecutor struct {
	tableName      string
	updateFields   []string
	conditionField string
	batchSize      int
	valuesBatch    [][]interface{}
	
	// String builder for query construction
	sb *strings.Builder
}

// NewBatchUpdate creates a new batch update executor
func NewBatchUpdate(tableName string, updateFields []string, conditionField string, batchSize int) *BatchUpdateExecutor {
	if batchSize <= 0 {
		batchSize = BatchSize
	}
	
	return &BatchUpdateExecutor{
		tableName:      tableName,
		updateFields:   updateFields,
		conditionField: conditionField,
		batchSize:      batchSize,
		valuesBatch:    make([][]interface{}, 0, batchSize),
		sb:             &strings.Builder{},
	}
}

// Add adds a record to the batch
func (b *BatchUpdateExecutor) Add(values map[string]interface{}, condition interface{}) error {
	// Extract values in correct field order
	rowValues := make([]interface{}, len(b.updateFields)+1) // +1 for condition
	
	// Get values for update fields
	for i, field := range b.updateFields {
		value, ok := values[field]
		if !ok {
			return fmt.Errorf("missing field: %s", field)
		}
		rowValues[i] = value
	}
	
	// Add condition value (e.g., uuid for WHERE clause)
	rowValues[len(b.updateFields)] = condition
	
	// Add to batch
	b.valuesBatch = append(b.valuesBatch, rowValues)
	
	// If batch is full, execute it
	if len(b.valuesBatch) >= b.batchSize {
		return b.Flush()
	}
	
	return nil
}

// Flush executes the current batch
func (b *BatchUpdateExecutor) Flush() error {
	if len(b.valuesBatch) == 0 {
		return nil // Nothing to do
	}
	
	// For better performance, we'll use a case statement to update multiple records in one query
	// This dramatically reduces the number of round trips to the database
	b.sb.Reset()
	b.sb.WriteString(`UPDATE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" SET `)
	
	// Add field assignments with placeholders
	for i, field := range b.updateFields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
		b.sb.WriteString(" = CASE ")
		b.sb.WriteString(b.conditionField)
		
		// For each row, add a WHEN clause
		paramCount := len(b.updateFields) + 1 // Add 1 for condition
		for rowIdx := range b.valuesBatch {
			b.sb.WriteString(fmt.Sprintf(" WHEN $%d THEN $%d", 
				rowIdx*paramCount + paramCount, // Condition parameter index 
				rowIdx*paramCount + i + 1))    // Value parameter index
		}
		
		// Close the CASE statement
		b.sb.WriteString(" ELSE ")
		b.sb.WriteString(field)
		b.sb.WriteString(" END")
	}
	
	// Add WHERE clause to limit updates to matching records
	b.sb.WriteString(` WHERE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`".`)
	b.sb.WriteString(b.conditionField)
	b.sb.WriteString(" IN (")
	
	// Add condition values
	paramCount := len(b.updateFields) + 1
	for rowIdx := range b.valuesBatch {
		if rowIdx > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(fmt.Sprintf("$%d", rowIdx*paramCount + paramCount))
	}
	b.sb.WriteString(")")
	
	// Flatten values for the query
	flatValues := make([]interface{}, 0, len(b.valuesBatch)*(len(b.updateFields)+1))
	for _, rowValues := range b.valuesBatch {
		flatValues = append(flatValues, rowValues...)
	}
	
	// Execute the query
	query := b.sb.String()
	_, err := Db.Exec(query, flatValues...)
	
	// Clear the batch
	b.valuesBatch = b.valuesBatch[:0]
	
	return err
}

// FlushWithTx executes the current batch within a transaction
func (b *BatchUpdateExecutor) FlushWithTx(tx *Tx) error {
	if len(b.valuesBatch) == 0 {
		return nil // Nothing to do
	}
	
	// For better performance, we'll use a case statement to update multiple records in one query
	// This dramatically reduces the number of round trips to the database
	b.sb.Reset()
	b.sb.WriteString(`UPDATE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`" SET `)
	
	// Add field assignments with placeholders
	for i, field := range b.updateFields {
		if i > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(field)
		b.sb.WriteString(" = CASE ")
		b.sb.WriteString(b.conditionField)
		
		// For each row, add a WHEN clause
		paramCount := len(b.updateFields) + 1 // Add 1 for condition
		for rowIdx := range b.valuesBatch {
			b.sb.WriteString(fmt.Sprintf(" WHEN $%d THEN $%d", 
				rowIdx*paramCount + paramCount, // Condition parameter index 
				rowIdx*paramCount + i + 1))    // Value parameter index
		}
		
		// Close the CASE statement
		b.sb.WriteString(" ELSE ")
		b.sb.WriteString(field)
		b.sb.WriteString(" END")
	}
	
	// Add WHERE clause to limit updates to matching records
	b.sb.WriteString(` WHERE "`)
	b.sb.WriteString(b.tableName)
	b.sb.WriteString(`".`)
	b.sb.WriteString(b.conditionField)
	b.sb.WriteString(" IN (")
	
	// Add condition values
	paramCount := len(b.updateFields) + 1
	for rowIdx := range b.valuesBatch {
		if rowIdx > 0 {
			b.sb.WriteString(", ")
		}
		b.sb.WriteString(fmt.Sprintf("$%d", rowIdx*paramCount + paramCount))
	}
	b.sb.WriteString(")")
	
	// Flatten values for the query
	flatValues := make([]interface{}, 0, len(b.valuesBatch)*(len(b.updateFields)+1))
	for _, rowValues := range b.valuesBatch {
		flatValues = append(flatValues, rowValues...)
	}
	
	// Execute the query using the transaction
	query := b.sb.String()
	_, err := tx.Exec(query, flatValues...)
	
	// Clear the batch
	b.valuesBatch = b.valuesBatch[:0]
	
	return err
}

// Pool for BatchInsertExecutors
var batchInsertPool = sync.Pool{
	New: func() interface{} {
		return &BatchInsertExecutor{
			valuesBatch: make([][]interface{}, 0, BatchSize),
			sb:          &strings.Builder{},
		}
	},
}

// GetBatchInsert gets a BatchInsertExecutor from the pool
func GetBatchInsert(tableName string, fields []string, batchSize int) *BatchInsertExecutor {
	b := batchInsertPool.Get().(*BatchInsertExecutor)
	
	// Reset and initialize
	b.tableName = tableName
	b.fields = fields
	b.valuesBatch = b.valuesBatch[:0]
	b.batchSize = batchSize
	b.returning = ""
	
	// Create field map for quick lookups
	b.fieldMap = make(map[string]bool, len(fields))
	for _, field := range fields {
		b.fieldMap[field] = true
	}
	
	return b
}

// ReleaseBatchInsert returns a BatchInsertExecutor to the pool
func ReleaseBatchInsert(b *BatchInsertExecutor) {
	// Clear references to allow GC
	b.valuesBatch = b.valuesBatch[:0]
	b.fieldMap = nil
	
	// Return to pool
	batchInsertPool.Put(b)
}