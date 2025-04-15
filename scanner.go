package fsql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
	
	"github.com/jmoiron/sqlx"
)

// FieldScanner is an optimized scanner for faster row processing
type FieldScanner struct {
	// Target field information
	dest        reflect.Value
	fieldMap    map[string][]int
	columns     []string
	fieldValues []interface{}
	
	// Object pools
	fieldMapPool sync.Map
}

// ColumnCache for pre-computed column mapping info
type ColumnCache struct {
	columnIndexes map[string]int
	fieldIndices  [][]int
	fieldPointers []interface{}
	initialized   bool
}

// Pool for column caches based on query patterns
var columnCachePool = sync.Pool{
	New: func() interface{} {
		return &ColumnCache{
			columnIndexes: make(map[string]int, 20),
			fieldIndices:  make([][]int, 0, 20),
			fieldPointers: make([]interface{}, 0, 20),
		}
	},
}

// Column cache map for faster lookups
var (
	// Map of column fingerprints to cached mapping info
	// Key: columnFingerprint, Value: *ColumnCache
	columnCacheMap sync.Map
	
	// Mutex for cache operations
	columnCacheMutex sync.RWMutex
)

// ScanRows efficiently scans rows into a destination struct or slice
// This is an optimized drop-in replacement for sqlx's StructScan
func ScanRows(rows *sqlx.Rows, dest interface{}) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer")
	}
	
	// Get the pointed-to value
	direct := reflect.Indirect(value)
	
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	
	// Create a fingerprint of the columns to use as cache key
	columnFingerprint := getColumnFingerprint(columns)
	
	// Check if we have a cached scanner for this column set
	var cache *ColumnCache
	cacheEntry, found := columnCacheMap.Load(columnFingerprint)
	if found {
		cache = cacheEntry.(*ColumnCache)
	} else {
		// Create a new cache entry
		cache = columnCachePool.Get().(*ColumnCache)
		cache.columnIndexes = make(map[string]int, len(columns))
		cache.fieldIndices = make([][]int, len(columns))
		cache.fieldPointers = make([]interface{}, len(columns))
		cache.initialized = false
		
		// Store in cache for future use
		columnCacheMap.Store(columnFingerprint, cache)
	}
	
	// For slice destinations, handle differently
	if direct.Kind() == reflect.Slice {
		sliceType := direct.Type().Elem()
		isPtr := sliceType.Kind() == reflect.Ptr
		
		for rows.Next() {
			// Create a new item
			var rowDest reflect.Value
			if isPtr {
				rowDest = reflect.New(sliceType.Elem())
			} else {
				rowDest = reflect.New(sliceType)
			}
			
			// Initialize cache if needed
			if !cache.initialized {
				err = initializeCache(cache, columns, rowDest)
				if err != nil {
					return err
				}
			}
			
			// Scan into the row destination using cached mapping
			err = scanWithCache(cache, rows)
			if err != nil {
				return err
			}
			
			// Append to the result slice
			if isPtr {
				direct.Set(reflect.Append(direct, rowDest))
			} else {
				direct.Set(reflect.Append(direct, reflect.Indirect(rowDest)))
			}
		}
		
		return rows.Err()
	} else {
		// For a single struct destination
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return sql.ErrNoRows
		}
		
		// Initialize cache if needed
		if !cache.initialized {
			err = initializeCache(cache, columns, value)
			if err != nil {
				return err
			}
		}
		
		// Scan into destination using cached mapping
		err = scanWithCache(cache, rows)
		if err != nil {
			return err
		}
		
		// Check if there are more rows (which would be unexpected)
		if rows.Next() {
			return errors.New("query returned multiple rows for a single destination")
		}
		
		return rows.Err()
	}
}

// Initialize the column mapping cache
func initializeCache(cache *ColumnCache, columns []string, value reflect.Value) error {
	// Get the pointed-to value
	elem := reflect.Indirect(value)
	elemType := elem.Type()
	
	// Get field mapping
	fieldMap := getFieldMap(elemType)
	
	// Create column-to-field mapping
	for i, colName := range columns {
		cache.columnIndexes[colName] = i
		
		// Find field indices for this column
		fieldPath, ok := fieldMap[colName]
		if !ok {
			// If column doesn't map to a field, use a placeholder
			var placeholder sql.RawBytes
			cache.fieldPointers[i] = &placeholder
			continue
		}
		
		// Store field indices for this column
		cache.fieldIndices[i] = fieldPath
		
		// Create scanner pointer for this field
		field := elem
		for _, idx := range fieldPath {
			field = field.Field(idx)
			if field.Kind() == reflect.Ptr && field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			if field.Kind() == reflect.Ptr {
				field = field.Elem()
			}
		}
		
		// Create a suitable destination pointer based on field type
		cache.fieldPointers[i] = getDestPtr(field)
	}
	
	cache.initialized = true
	return nil
}

// Scan a row using the cached mapping
func scanWithCache(cache *ColumnCache, rows *sqlx.Rows) error {
	return rows.Scan(cache.fieldPointers...)
}

// Get a field map for a given type
var fieldMapCache sync.Map

func getFieldMap(t reflect.Type) map[string][]int {
	// Check if we already have this type in cache
	if cached, ok := fieldMapCache.Load(t); ok {
		return cached.(map[string][]int)
	}
	
	fieldMap := make(map[string][]int)
	
	// Create field map by recursively analyzing the type
	buildFieldMap(t, fieldMap, nil)
	
	// Store in cache for future use
	fieldMapCache.Store(t, fieldMap)
	
	return fieldMap
}

// Build a field map by recursively analyzing the type
func buildFieldMap(t reflect.Type, fieldMap map[string][]int, path []int) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		
		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}
		
		// Get DB tag
		tag := field.Tag.Get("db")
		if tag == "" || tag == "-" {
			// If this field is a struct, recurse into it
			if field.Type.Kind() == reflect.Struct {
				// Add this field index to the path
				newPath := make([]int, len(path)+1)
				copy(newPath, path)
				newPath[len(path)] = i
				
				// Recurse
				buildFieldMap(field.Type, fieldMap, newPath)
			}
			continue
		}
		
		// Create the field path
		fieldPath := make([]int, len(path)+1)
		copy(fieldPath, path)
		fieldPath[len(path)] = i
		
		// Store the field path
		fieldMap[tag] = fieldPath
	}
}

// Create a destination pointer for a field
func getDestPtr(field reflect.Value) interface{} {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var v sql.NullInt64
		return &v
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var v sql.NullInt64
		return &v
	case reflect.Float32, reflect.Float64:
		var v sql.NullFloat64
		return &v
	case reflect.Bool:
		var v sql.NullBool
		return &v
	case reflect.String:
		var v sql.NullString
		return &v
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			var v sql.NullTime
			return &v
		}
		// For other structs, return a pointer to the field
		return field.Addr().Interface()
	default:
		// For complex types, return a pointer to the field
		return field.Addr().Interface()
	}
}

// Generate a fingerprint for a set of columns
func getColumnFingerprint(columns []string) string {
	// Use a string builder for better performance
	var fingerprint string
	if len(columns) < 5 {
		// For small column sets, just join them
		fingerprint = ""
		for i, col := range columns {
			if i > 0 {
				fingerprint += "|"
			}
			fingerprint += col
		}
	} else {
		// For larger sets, use the first column plus length as a quick fingerprint
		fingerprint = columns[0] + "|" + columns[len(columns)-1] + "|" + fmt.Sprintf("%d", len(columns))
	}
	return fingerprint
}

// ScanSingle scans a single row into the destination struct
func ScanSingle(rows *sqlx.Rows, dest interface{}) error {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	
	err := ScanRows(rows, dest)
	if err != nil {
		return err
	}
	
	if rows.Next() {
		return errors.New("query returned multiple rows for a single destination")
	}
	
	return rows.Err()
}