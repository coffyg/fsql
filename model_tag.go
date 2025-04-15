// model_tag.go
package fsql

import (
	"fmt"
	"reflect"
	"sync"
)

// ModelTagCache is a cache for model field information
type ModelTagCache struct {
	Fields []ModelField
}

// ModelField holds information about a struct field
type ModelField struct {
	Name        string // Field name
	DbName      string // Database column name
	Mode        string // Insert, update, linked modes
	InsertValue string // Default value for inserts
}

// Struct metadata cache
var (
	modelTagCacheMutex sync.RWMutex
	modelTagCache      = make(map[reflect.Type]*ModelTagCache)
)

// InitModelTagCacheForType initializes the model tag cache for a struct type
func InitModelTagCacheForType(modelType reflect.Type) (*ModelTagCache, error) {
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	
	if modelType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct, got %s", modelType.Kind())
	}
	
	// Check if already cached
	modelTagCacheMutex.RLock()
	cache, exists := modelTagCache[modelType]
	modelTagCacheMutex.RUnlock()
	
	if exists {
		return cache, nil
	}
	
	// Parse struct fields
	numFields := modelType.NumField()
	fields := make([]ModelField, 0, numFields)
	
	for i := 0; i < numFields; i++ {
		field := modelType.Field(i)
		
		// Get database tag
		dbTag := field.Tag.Get("db")
		if dbTag == "" || dbTag == "-" {
			continue
		}
		
		// Get mode and insert value
		modeTag := field.Tag.Get("dbMode")
		insertValueTag := field.Tag.Get("dbInsertValue")
		
		// Create field info
		fields = append(fields, ModelField{
			Name:        field.Name,
			DbName:      dbTag,
			Mode:        modeTag,
			InsertValue: insertValueTag,
		})
	}
	
	// Store in cache
	cache = &ModelTagCache{
		Fields: fields,
	}
	
	modelTagCacheMutex.Lock()
	modelTagCache[modelType] = cache
	modelTagCacheMutex.Unlock()
	
	return cache, nil
}

// getModelTagCache retrieves the model tag cache for a struct type
func getModelTagCache(modelType reflect.Type) (*ModelTagCache, error) {
	// Check if already cached
	modelTagCacheMutex.RLock()
	cache, exists := modelTagCache[modelType]
	modelTagCacheMutex.RUnlock()
	
	if exists {
		return cache, nil
	}
	
	// Create a temporary instance to initialize
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	
	if modelType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct, got %s", modelType.Kind())
	}
	
	// Initialize and retrieve from cache
	return InitModelTagCacheForType(modelType)
}