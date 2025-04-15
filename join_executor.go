// join_executor.go
package fsql

import (
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// JoinQueryExecutor provides optimized execution of join queries
type JoinQueryExecutor struct {
	// State for tracking prepared statements
	stmtCache sync.Map
	// Cache for query structure analysis
	queryStructureCache sync.Map
	// Stats for monitoring
	cacheHits   int64
	cacheMisses int64
}

// Global instance for join optimization
var joinExecutor = &JoinQueryExecutor{}

// Object pools for join query execution
var (
	// Pool for argument slices to reduce allocations in query execution
	argsPool = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, 8) // Common size for query args
		},
	}
	
	// Pool for temporary string builders used in query analysis
	joinQueryBuilderPool = sync.Pool{
		New: func() interface{} {
			sb := strings.Builder{}
			sb.Grow(256) // Typical join query size
			return &sb
		},
	}
	
	// Pool for join table mappings
	joinTablesPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string, 4) // Typical number of joined tables
		},
	}
	
	// Common join keywords for fast detection
	joinKeywords = []string{" JOIN ", " LEFT JOIN ", " RIGHT JOIN ", " INNER JOIN "}
)

// ExecuteJoinQueryWithCache executes a join query with caching
// This method is safer than directly modifying the existing query builder
func ExecuteJoinQueryWithCache(query string, args []interface{}, result interface{}) error {
	// Use the optimized version by default for better performance
	return ExecuteJoinQueryOptimized(query, args, result)
}

// ExecuteJoinQueryOptimized executes a join query with full optimization
func ExecuteJoinQueryOptimized(query string, args []interface{}, result interface{}) error {
	// Only optimize if the query contains JOIN
	isJoin := isJoinQuery(query)
	
	// Fast path for non-join queries
	if !isJoin {
		return Db.Select(result, query, args...)
	}
	
	// Use cached query string and reuse argument slice
	cachedQuery, cachedArgs := CachedQuery(query, args)
	
	// Optimize for slice destination type by pre-allocating if possible
	resultType := reflect.TypeOf(result)
	if resultType.Kind() == reflect.Ptr && resultType.Elem().Kind() == reflect.Slice {
		// Pre-allocate the slice if it's empty to reduce reallocations
		sliceVal := reflect.ValueOf(result).Elem()
		if sliceVal.Len() == 0 && sliceVal.Cap() < 32 {
			// Pre-allocate to a reasonable size if currently empty
			newSlice := reflect.MakeSlice(sliceVal.Type(), 0, 32)
			sliceVal.Set(newSlice)
		}
	}
	
	// Update statistics
	if cachedQuery != query {
		atomic.AddInt64(&joinExecutor.cacheHits, 1)
	} else {
		atomic.AddInt64(&joinExecutor.cacheMisses, 1)
	}
	
	// Use prepared statement with optimized scanner for better performance
	return ExecuteWithPrepared(cachedQuery, cachedArgs, result)
}

// isJoinQuery determines if a query is a join query (optimized implementation)
// This version uses efficient string searching without allocations
func isJoinQuery(query string) bool {
	for _, keyword := range joinKeywords {
		if strings.Contains(query, keyword) {
			return true
		}
	}
	return false
}

// Model caching to reduce struct analysis overhead
var modelStructCache = sync.Map{}

// FindModelFields pre-analyzes models to speed up field mapping
func FindModelFields(modelType interface{}) {
	// Get model name using optimized type check
	modelName := getModelTypeName(modelType)
	
	// Skip if already analyzed (fast path using sync.Map)
	if _, ok := modelStructCache.Load(modelName); ok {
		return
	}
	
	// Analyze model structure and store the result
	structFields := analyzeModelStructure(modelType)
	modelStructCache.Store(modelName, structFields)
}

// getModelTypeName gets a string representation of model type
func getModelTypeName(modelType interface{}) string {
	if modelType == nil {
		return "nil"
	}
	
	t := reflect.TypeOf(modelType)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	
	return t.String()
}

// analyzeModelStructure analyzes a model's structure for fast field mapping
// This reduces the need for repeated reflection during query execution
func analyzeModelStructure(modelType interface{}) map[string][]int {
	fieldMap := make(map[string][]int)
	
	t := reflect.TypeOf(modelType)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	
	// Only process struct types
	if t.Kind() != reflect.Struct {
		return fieldMap
	}
	
	// Analyze fields and store their indices for fast mapping
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		
		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}
		
		// Get the db tag or use field name
		tag := field.Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}
		
		// Split the tag by comma and use the first part as the field name
		dbName := strings.Split(tag, ",")[0]
		fieldMap[dbName] = append([]int{}, i)
	}
	
	return fieldMap
}