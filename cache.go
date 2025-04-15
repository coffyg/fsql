// cache.go
package fsql

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/coffyg/utils"
)

// modelFieldsCache is a global cache for model metadata
var modelFieldsCache = utils.NewOptimizedSafeMap[*modelInfo]()

// fieldsCacheKey provides a cache key for quoted field names
type fieldsCacheKey struct {
	tableName     string
	aliasTableName string
	mode          string
}

// fieldsCache caches generated field strings to avoid repeated string operations
var fieldsCache = utils.NewOptimizedSafeMap[cachedFields]()

type cachedFields struct {
	fields     []string
	fieldNames []string
}

// modeFlags is a bitset for efficient mode checking
type modeFlags uint8

const (
	modeInsert modeFlags = 1 << iota
	modeUpdate
	modeSelect
	modeLink
	modeSkip
)

// modelInfo holds the metadata for a model
type modelInfo struct {
	dbTagMap          map[string]string
	dbInsertValueMap  map[string]string
	dbFieldsSelect    []string
	dbFieldsInsert    []string
	dbFieldsUpdate    []string
	dbFieldsSelectMap map[string]struct{}
	dbFieldsInsertMap map[string]struct{}
	dbFieldsUpdateMap map[string]struct{}
	linkedFields      map[string]string // FieldName -> TableAlias
	
	// Pre-generated quoted strings for faster access
	quotedTableName string
	quotedFields    map[string]string
}

// stringReplacer is a cached instance for faster string replacement
var (
	quotesReplacer    = strings.NewReplacer(`"`, ``)
	tagParserPool     sync.Pool
	fieldsBuilderPool sync.Pool
	initOnce          sync.Once
)

func init() {
	// Initialize pools
	initOnce.Do(func() {
		tagParserPool = sync.Pool{
			New: func() interface{} {
				return make(map[string]bool, 4) // Pre-allocate for common case
			},
		}
		
		fieldsBuilderPool = sync.Pool{
			New: func() interface{} {
				return &strings.Builder{}
			},
		}
	})
}

// InitModelTagCache initializes the model metadata cache
func InitModelTagCache(model interface{}, tableName string) {
	// Fast path: check if already initialized
	if _, exists := getModelInfo(tableName); exists {
		return
	}

	modelType := getModelType(model)

	// Pre-allocate maps with reasonable capacity to reduce resizing
	dbTagMap := make(map[string]string, modelType.NumField())
	dbInsertValueMap := make(map[string]string, modelType.NumField()/2)
	quotedFields := make(map[string]string, modelType.NumField())
	
	// Pre-allocate slices with reasonable capacity
	dbFieldsSelect := make([]string, 0, modelType.NumField())
	dbFieldsInsert := make([]string, 0, modelType.NumField())
	dbFieldsUpdate := make([]string, 0, modelType.NumField())
	
	dbFieldsSelectMap := make(map[string]struct{}, modelType.NumField())
	dbFieldsInsertMap := make(map[string]struct{}, modelType.NumField())
	dbFieldsUpdateMap := make(map[string]struct{}, modelType.NumField())
	linkedFields := make(map[string]string, modelType.NumField()/4) // Assuming ~25% are linked

	// Pre-compute the quoted table name for reuse
	quotedTableName := `"` + quotesReplacer.Replace(tableName) + `"`

	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		dbTagValue := field.Tag.Get("db")
		if dbTagValue == "" || dbTagValue == "-" {
			continue
		}

		// Pre-compute quoted field name
		quotedFieldName := `"` + quotesReplacer.Replace(dbTagValue) + `"`
		quotedFields[dbTagValue] = quotedFieldName

		dbMode := field.Tag.Get("dbMode")
		dbInsertValue := field.Tag.Get("dbInsertValue")
		
		// Use bit flags for more efficient mode checking
		var flags modeFlags
		
		// Get a parser from pool to avoid allocations
		modeParser := tagParserPool.Get().(map[string]bool)
		// Clear the map for reuse
		for k := range modeParser {
			delete(modeParser, k)
		}
		
		// Parse mode flags
		modes := strings.Split(dbMode, ",")
		for _, mode := range modes {
			modeParser[mode] = true
		}

		// Convert string-based flags to bit flags for faster checking
		if modeParser["i"] {
			flags |= modeInsert
		}
		if modeParser["u"] {
			flags |= modeUpdate
		}
		if modeParser["s"] {
			flags |= modeSkip
		}
		if modeParser["l"] || modeParser["link"] {
			flags |= modeLink
		}
		
		// Return parser to pool
		tagParserPool.Put(modeParser)

		if (flags & modeLink) != 0 {
			// Handle linked fields
			linkedFields[field.Name] = dbTagValue
			continue
		}

		dbTagMap[field.Name] = dbTagValue

		if (flags & modeSkip) != 0 {
			continue
		}

		if (flags & modeInsert) != 0 {
			dbFieldsInsert = append(dbFieldsInsert, dbTagValue)
			dbFieldsInsertMap[dbTagValue] = struct{}{}
			if dbInsertValue != "" {
				dbInsertValueMap[dbTagValue] = dbInsertValue
			}
		}
		if (flags & modeUpdate) != 0 {
			dbFieldsUpdate = append(dbFieldsUpdate, dbTagValue)
			dbFieldsUpdateMap[dbTagValue] = struct{}{}
		}
		dbFieldsSelect = append(dbFieldsSelect, dbTagValue)
		dbFieldsSelectMap[dbTagValue] = struct{}{}
	}

	modelInfo := &modelInfo{
		dbTagMap:          dbTagMap,
		dbInsertValueMap:  dbInsertValueMap,
		dbFieldsSelect:    dbFieldsSelect,
		dbFieldsInsert:    dbFieldsInsert,
		dbFieldsUpdate:    dbFieldsUpdate,
		dbFieldsSelectMap: dbFieldsSelectMap,
		dbFieldsInsertMap: dbFieldsInsertMap,
		dbFieldsUpdateMap: dbFieldsUpdateMap,
		linkedFields:      linkedFields,
		quotedTableName:   quotedTableName,
		quotedFields:      quotedFields,
	}

	modelFieldsCache.Set(tableName, modelInfo)
}

// getModelInfo retrieves model info from cache
func getModelInfo(tableName string) (*modelInfo, bool) {
	return modelFieldsCache.Get(tableName)
}

// getModelType extracts the struct type from an interface
func getModelType(model interface{}) reflect.Type {
	modelType := reflect.TypeOf(model)
	
	// Dereference pointers until we reach a non-pointer type
	for modelType != nil && modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType == nil || modelType.Kind() != reflect.Struct {
		panic(fmt.Sprintf("expected a struct, got %T", model))
	}

	return modelType
}

// getCachedFieldsByMode gets or computes fields and caches the result
func getCachedFieldsByMode(tableName, mode, aliasTableName string) ([]string, []string) {
	// Create cache key
	key := tableName + "|" + mode + "|" + aliasTableName
	
	// Check cache first
	if cached, ok := fieldsCache.Get(key); ok {
		return cached.fields, cached.fieldNames
	}
	
	// Not in cache, compute fields
	fields, fieldNames := computeFieldsByMode(tableName, mode, aliasTableName)
	
	// Cache the result
	fieldsCache.Set(key, cachedFields{
		fields:     fields,
		fieldNames: fieldNames,
	})
	
	return fields, fieldNames
}

// computeFieldsByMode generates the SQL field selectors for a given mode
func computeFieldsByMode(tableName, mode, aliasTableName string) ([]string, []string) {
	modelInfo, ok := getModelInfo(tableName)
	if !ok {
		panic("table name not initialized: " + tableName)
	}

	var dbFields []string
	switch mode {
	case "select":
		dbFields = modelInfo.dbFieldsSelect
	case "insert":
		dbFields = modelInfo.dbFieldsInsert
	case "update":
		dbFields = modelInfo.dbFieldsUpdate
	default:
		panic("invalid mode: " + mode)
	}

	// Pre-allocate slices with exact capacity
	fields := make([]string, 0, len(dbFields))
	fieldNames := make([]string, 0, len(dbFields))
	
	// Get a string builder from pool
	sb := fieldsBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	
	// Clean the alias table name once
	cleanAliasName := ""
	if aliasTableName != "" {
		cleanAliasName = quotesReplacer.Replace(aliasTableName)
	}

	for _, fieldName := range dbFields {
		quotedFieldName := modelInfo.quotedFields[fieldName]
		
		sb.Reset()
		if aliasTableName != "" {
			sb.WriteString(`"`)
			sb.WriteString(cleanAliasName)
			sb.WriteString(`".`)
			sb.WriteString(quotedFieldName)
			sb.WriteString(` AS "`)
			sb.WriteString(cleanAliasName)
			sb.WriteString(`.`)
			sb.WriteString(fieldName)
			sb.WriteString(`"`)
		} else {
			sb.WriteString(modelInfo.quotedTableName)
			sb.WriteString(`.`)
			sb.WriteString(quotedFieldName)
		}
		
		fields = append(fields, sb.String())
		fieldNames = append(fieldNames, fieldName)
	}
	
	// Return builder to pool
	fieldsBuilderPool.Put(sb)

	return fields, fieldNames
}

// GetSelectFields returns the column selectors for SELECT queries
func GetSelectFields(tableName, aliasTableName string) ([]string, []string) {
	return getCachedFieldsByMode(tableName, "select", aliasTableName)
}

// GetInsertFields returns the column names for INSERT queries
func GetInsertFields(tableName string) ([]string, []string) {
	return getCachedFieldsByMode(tableName, "insert", "")
}

// GetUpdateFields returns the column names for UPDATE queries
func GetUpdateFields(tableName string) ([]string, []string) {
	return getCachedFieldsByMode(tableName, "update", "")
}

// GetInsertValues returns the default values for INSERT queries
func GetInsertValues(tableName string) map[string]string {
	modelInfo, ok := getModelInfo(tableName)
	if !ok {
		panic("table name not initialized: " + tableName)
	}
	return modelInfo.dbInsertValueMap
}
