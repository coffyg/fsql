// join_optimizer.go
package fsql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// JoinResultCache maintains a cache of join query results
type JoinResultCache struct {
	cache      map[string]interface{}
	expiration map[string]time.Time
	ttl        time.Duration
	mutex      sync.RWMutex
}

// NewJoinResultCache creates a new join query cache
func NewJoinResultCache(ttl time.Duration) *JoinResultCache {
	return &JoinResultCache{
		cache:      make(map[string]interface{}),
		expiration: make(map[string]time.Time),
		ttl:        ttl,
	}
}

// Get retrieves a result from the cache
func (c *JoinResultCache) Get(key string) (interface{}, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	result, found := c.cache[key]
	if !found {
		return nil, false
	}
	
	exp, _ := c.expiration[key]
	if time.Now().After(exp) {
		// Entry expired
		return nil, false
	}
	
	return result, true
}

// Set adds a result to the cache
func (c *JoinResultCache) Set(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	c.cache[key] = value
	c.expiration[key] = time.Now().Add(c.ttl)
}

// OptimizeJoinQuery optimizes a join query for better performance
func OptimizeJoinQuery(query string) string {
	// Analyze query structure
	isSimpleJoin := strings.Contains(query, "JOIN") && !strings.Contains(query, "OUTER JOIN")
	
	if isSimpleJoin {
		// For simple joins, add an optimization hint
		return query + " /*+ HASH_JOIN */"
	}
	
	return query
}

// Batch of IDs for efficient loading
type IdBatch struct {
	TableName string
	Ids       []string
	KeyField  string
}

// ExecuteJoinQuery executes a join query with optimizations
func ExecuteJoinQuery(ctx context.Context, query string, args []interface{}, result interface{}) error {
	// Apply query optimizations
	optimizedQuery := OptimizeJoinQuery(query)
	
	// Use the cache for frequently executed queries
	cachedQuery, cachedArgs := CachedQuery(optimizedQuery, args)
	
	// Get result type information
	resultType := reflect.TypeOf(result)
	if resultType.Kind() != reflect.Ptr || resultType.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("result must be a pointer to a slice")
	}
	
	// Execute the query
	return Db.SelectContext(ctx, result, cachedQuery, cachedArgs...)
}

// Object pool for join operations
var (
	joinResultCache = NewJoinResultCache(5 * time.Minute)
	joinQueryMutex  sync.Mutex
	joinPlanCache   = make(map[string]string)
)

// JoinPlan represents a precomputed join execution plan
type JoinPlan struct {
	MainQuery    string
	JoinQueries  []string
	JoinKeyField string
}

// optimizeJoins reduces allocation overhead in queries with joins
func optimizeJoins() {
	// This optimizes future join operations
	joinQueryMutex.Lock()
	defer joinQueryMutex.Unlock()
	
	// Pre-create optimized join plans for common table combinations
	joinPlanCache["website_realm"] = `
		SELECT w.*, r.uuid as r_uuid, r.name as r_name, r.created_at as r_created_at, r.updated_at as r_updated_at
		FROM website w
		LEFT JOIN realm r ON w.realm_uuid = r.uuid
		/*+ HASH_JOIN */
	`
}

func init() {
	// Initialize optimized joins
	optimizeJoins()
}