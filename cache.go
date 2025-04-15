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

// modeFlags is a bitset for efficient mode checking
type modeFlags uint8

const (
	modeInsert modeFlags = 1 << iota
	modeUpdate
	modeSelect
	modeLink
	modeSkip
)

// Pre-allocate slice and map capacities based on typical field counts
const (
	defaultFieldCount   = 16 // Typical number of fields in a model
	defaultLinkedFields = 4  // Typical number of linked fields
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
	
	// Pre-computed field strings for common operations
	selectFieldsCache      map[string][]string // key: alias, value: field strings
	selectFieldNamesCache  map[string][]string // key: alias, value: field names
	insertFieldsCache      []string
	insertFieldNamesCache  []string
	updateFieldsCache      []string
	updateFieldNamesCache  []string
}

// stringReplacer is a cached instance for faster string replacement
var (
	quotesReplacer       = strings.NewReplacer(`"`, ``)
	tagParserPool        sync.Pool
	fieldsBuilderPool    sync.Pool
	stringSlicePool      sync.Pool
	selectFieldMapPool   sync.Pool
	selectFieldNamePool  sync.Pool
	fieldStringCachePool sync.Pool
	initOnce             sync.Once
)

func init() {
	// Initialize pools with pre-sized objects
	initOnce.Do(func() {
		tagParserPool = sync.Pool{
			New: func() interface{} {
				return make(map[string]bool, 4) // Pre-allocate for common mode flags
			},
		}
		
		fieldsBuilderPool = sync.Pool{
			New: func() interface{} {
				sb := &strings.Builder{}
				sb.Grow(128) // Pre-allocate a reasonable buffer size
				return sb
			},
		}
		
		stringSlicePool = sync.Pool{
			New: func() interface{} {
				return make([]string, 0, defaultFieldCount)
			},
		}
		
		selectFieldMapPool = sync.Pool{
			New: func() interface{} {
				return make(map[string][]string, 4) // Common number of aliases
			},
		}
		
		selectFieldNamePool = sync.Pool{
			New: func() interface{} {
				return make(map[string][]string, 4) // Common number of aliases
			},
		}
		
		fieldStringCachePool = sync.Pool{
			New: func() interface{} {
				return make(map[string]string, defaultFieldCount)
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
	numFields := modelType.NumField()

	// Pre-allocate maps with exact capacity to reduce resizing
	dbTagMap := make(map[string]string, numFields)
	dbInsertValueMap := make(map[string]string, numFields/2)
	quotedFields := make(map[string]string, numFields)
	
	// Pre-allocate slices with exact capacity
	dbFieldsSelect := make([]string, 0, numFields)
	dbFieldsInsert := make([]string, 0, numFields)
	dbFieldsUpdate := make([]string, 0, numFields)
	
	dbFieldsSelectMap := make(map[string]struct{}, numFields)
	dbFieldsInsertMap := make(map[string]struct{}, numFields)
	dbFieldsUpdateMap := make(map[string]struct{}, numFields)
	linkedFields := make(map[string]string, numFields/4) // Assuming ~25% are linked

	// Pre-compute the quoted table name for reuse
	quotedTableName := `"` + quotesReplacer.Replace(tableName) + `"`

	// Get a parser from pool to avoid allocations
	modeParser := tagParserPool.Get().(map[string]bool)
	defer tagParserPool.Put(modeParser) // Return when done
	
	// Reuse a temporary string builder for quoted field names
	sb := fieldsBuilderPool.Get().(*strings.Builder)
	defer fieldsBuilderPool.Put(sb)
	
	// Pre-allocate field caches
	selectFieldsCache := make(map[string][]string, 4) // Common aliases: "", "t", "a", etc
	selectFieldNamesCache := make(map[string][]string, 4)
	
	// Populate empty alias cache entries in advance
	selectFieldsCache[""] = make([]string, 0, numFields)
	selectFieldNamesCache[""] = make([]string, 0, numFields)

	for i := 0; i < numFields; i++ {
		field := modelType.Field(i)
		dbTagValue := field.Tag.Get("db")
		if dbTagValue == "" || dbTagValue == "-" {
			continue
		}

		// Clear the map for reuse
		for k := range modeParser {
			delete(modeParser, k)
		}
		
		// Pre-compute quoted field name
		sb.Reset()
		sb.WriteString(`"`)
		sb.WriteString(quotesReplacer.Replace(dbTagValue))
		sb.WriteString(`"`)
		quotedFieldName := sb.String()
		quotedFields[dbTagValue] = quotedFieldName

		dbMode := field.Tag.Get("dbMode")
		dbInsertValue := field.Tag.Get("dbInsertValue")
		
		// Use bit flags for mode checking - faster than string comparison
		var flags modeFlags
		
		// Parse mode flags in a single pass without strings.Split
		if dbMode != "" {
			start := 0
			for i := 0; i <= len(dbMode); i++ {
				if i == len(dbMode) || dbMode[i] == ',' {
					mode := dbMode[start:i]
					modeParser[mode] = true
					start = i + 1
				}
			}
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

	// Pre-compute field strings for the empty alias case (most common)
	insertFieldsCache, insertFieldNamesCache := computeFieldsByModeInternal(
		dbFieldsInsert, quotedTableName, quotedFields, "")
	updateFieldsCache, updateFieldNamesCache := computeFieldsByModeInternal(
		dbFieldsUpdate, quotedTableName, quotedFields, "")
	
	// Compute and cache select fields for empty alias
	emptySelectFields, emptySelectFieldNames := computeFieldsByModeInternal(
		dbFieldsSelect, quotedTableName, quotedFields, "")
	selectFieldsCache[""] = emptySelectFields
	selectFieldNamesCache[""] = emptySelectFieldNames

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
		
		// Cache pre-computed field strings
		selectFieldsCache:      selectFieldsCache,
		selectFieldNamesCache:  selectFieldNamesCache,
		insertFieldsCache:      insertFieldsCache,
		insertFieldNamesCache:  insertFieldNamesCache,
		updateFieldsCache:      updateFieldsCache,
		updateFieldNamesCache:  updateFieldNamesCache,
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

// computeFieldsByModeInternal generates field strings without table lookup
// This is a helper function used internally during initialization
func computeFieldsByModeInternal(
	dbFields []string,
	quotedTableName string,
	quotedFields map[string]string,
	aliasTableName string,
) ([]string, []string) {
	// Pre-allocate slices with exact capacity
	fields := make([]string, 0, len(dbFields))
	fieldNames := make([]string, 0, len(dbFields))
	
	// Get a string builder from pool
	sb := fieldsBuilderPool.Get().(*strings.Builder)
	defer fieldsBuilderPool.Put(sb)
	
	// Clean the alias table name once
	cleanAliasName := ""
	if aliasTableName != "" {
		cleanAliasName = quotesReplacer.Replace(aliasTableName)
	}

	for _, fieldName := range dbFields {
		quotedFieldName := quotedFields[fieldName]
		
		sb.Reset()
		if aliasTableName != "" {
			// Pre-allocate a reasonable buffer size for the field string
			sb.Grow(len(cleanAliasName)*2 + len(quotedFieldName) + len(fieldName) + 20)
			
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
			// Pre-allocate a reasonable buffer size
			sb.Grow(len(quotedTableName) + len(quotedFieldName) + 2)
			
			sb.WriteString(quotedTableName)
			sb.WriteString(`.`)
			sb.WriteString(quotedFieldName)
		}
		
		fields = append(fields, sb.String())
		fieldNames = append(fieldNames, fieldName)
	}

	return fields, fieldNames
}

// GetSelectFields returns the column selectors for SELECT queries
func GetSelectFields(tableName, aliasTableName string) ([]string, []string) {
	modelInfo, ok := getModelInfo(tableName)
	if !ok {
		panic("table name not initialized: " + tableName)
	}
	
	// First check if pre-computed fields are available in the cache
	if cachedFields, ok := modelInfo.selectFieldsCache[aliasTableName]; ok {
		return cachedFields, modelInfo.selectFieldNamesCache[aliasTableName]
	}
	
	// Compute fields for this alias and cache them
	fields, fieldNames := computeFieldsByModeInternal(
		modelInfo.dbFieldsSelect,
		modelInfo.quotedTableName,
		modelInfo.quotedFields,
		aliasTableName,
	)
	
	// Store in the model's cache for future use
	modelInfo.selectFieldsCache[aliasTableName] = fields
	modelInfo.selectFieldNamesCache[aliasTableName] = fieldNames
	
	return fields, fieldNames
}

// GetInsertFields returns the column names for INSERT queries
func GetInsertFields(tableName string) ([]string, []string) {
	modelInfo, ok := getModelInfo(tableName)
	if !ok {
		panic("table name not initialized: " + tableName)
	}
	
	// Return pre-computed values directly
	return modelInfo.insertFieldsCache, modelInfo.insertFieldNamesCache
}

// GetUpdateFields returns the column names for UPDATE queries
func GetUpdateFields(tableName string) ([]string, []string) {
	modelInfo, ok := getModelInfo(tableName)
	if !ok {
		panic("table name not initialized: " + tableName)
	}
	
	// Return pre-computed values directly
	return modelInfo.updateFieldsCache, modelInfo.updateFieldNamesCache
}

// GetInsertValues returns the default values for INSERT queries
func GetInsertValues(tableName string) map[string]string {
	modelInfo, ok := getModelInfo(tableName)
	if !ok {
		panic("table name not initialized: " + tableName)
	}
	return modelInfo.dbInsertValueMap
}
