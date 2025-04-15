// query_cache.go
package fsql

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// QueryCacheEntry represents a cached query result
type QueryCacheEntry struct {
	Query     string
	Args      []interface{}
	CreatedAt time.Time
	LastUsed  time.Time
	UseCount  int64
}

// QueryCache is a simple LRU cache for query results
type QueryCache struct {
	entries        map[string]*QueryCacheEntry
	preparedStmt   map[string]*PreparedStatement
	maxSize        int
	ttl            time.Duration
	mutex          sync.RWMutex
	hits           int64
	misses         int64
	evictions      int64
	savedAllocations int64
}

// PreparedStatement represents a prepared query statement
type PreparedStatement struct {
	Query        string
	ParamIndexes []int
	CreatedAt    time.Time
	LastUsed     time.Time
	UseCount     int64
}

var (
	// Global query cache
	globalQueryCache = NewQueryCache(1000, 10*time.Minute)
	
	// Pre-compute parameter placeholders for common sizes
	paramPlaceholders = [][]string{
		{},                  // 0 params
		{"$1"},              // 1 param
		{"$1", "$2"},        // 2 params
		{"$1", "$2", "$3"},  // 3 params
		{"$1", "$2", "$3", "$4"}, // 4 params
		{"$1", "$2", "$3", "$4", "$5"}, // 5 params
	}
	
	// Object pools for query cache operations
	argSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, 8)
		},
	}
	
	hashBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 64)
		},
	}
	
	// Cache key buffer pool for temporary hash operations
	keyBufferPool = sync.Pool{
		New: func() interface{} {
			return sha256.New()
		},
	}
	
	// Pre-allocated hash buffer for reuse
	hashOutputBuf = make([]byte, sha256.Size)
)

// NewQueryCache creates a new query cache with specified capacity and TTL
func NewQueryCache(maxSize int, ttl time.Duration) *QueryCache {
	return &QueryCache{
		entries:      make(map[string]*QueryCacheEntry, maxSize),
		preparedStmt: make(map[string]*PreparedStatement, maxSize),
		maxSize:      maxSize,
		ttl:          ttl,
	}
}

// generateCacheKey creates a deterministic key for a query and its arguments
// This optimized version reduces allocations
func generateCacheKey(query string, args []interface{}) string {
	// Create a hash for the query and arguments
	h := sha256.New()
	h.Write([]byte(query))
	
	// Add arguments to the hash without fmt.Fprintf allocations
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			h.Write([]byte(v))
		case int:
			h.Write([]byte{byte(v & 0xFF), byte((v >> 8) & 0xFF), byte((v >> 16) & 0xFF), byte((v >> 24) & 0xFF)})
		case int64:
			h.Write([]byte{byte(v & 0xFF), byte((v >> 8) & 0xFF), byte((v >> 16) & 0xFF), byte((v >> 24) & 0xFF),
				byte((v >> 32) & 0xFF), byte((v >> 40) & 0xFF), byte((v >> 48) & 0xFF), byte((v >> 56) & 0xFF)})
		case float64:
			var buf [8]byte
			binary := *(*[8]byte)(unsafe.Pointer(&v))
			for i := 0; i < 8; i++ {
				buf[i] = binary[i]
			}
			h.Write(buf[:])
		default:
			// For other types, fallback to string representation
			h.Write([]byte(fmt.Sprintf("%v", v)))
		}
		// Separator between arguments
		h.Write([]byte{':'})
	}
	
	// Get the hash value
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}

// Get retrieves a cached query, returns nil if not found
func (c *QueryCache) Get(query string, args []interface{}) *QueryCacheEntry {
	key := generateCacheKey(query, args)
	
	c.mutex.RLock()
	entry, found := c.entries[key]
	c.mutex.RUnlock()
	
	if !found {
		atomic.AddInt64(&c.misses, 1)
		return nil
	}
	
	// Check if entry has expired
	now := time.Now()
	if now.Sub(entry.CreatedAt) > c.ttl {
		// Entry expired, remove it
		c.mutex.Lock()
		delete(c.entries, key)
		c.mutex.Unlock()
		atomic.AddInt64(&c.misses, 1)
		atomic.AddInt64(&c.evictions, 1)
		return nil
	}
	
	// Update usage stats - use atomic for thread safety with less locking
	atomic.AddInt64(&entry.UseCount, 1)
	entry.LastUsed = now  // Update last used time
	atomic.AddInt64(&c.hits, 1)
	atomic.AddInt64(&c.savedAllocations, 1)
	
	return entry
}

// Set adds a query to the cache
func (c *QueryCache) Set(query string, args []interface{}) *QueryCacheEntry {
	key := generateCacheKey(query, args)
	now := time.Now()
	
	// Clone arguments to prevent mutations
	clonedArgs := CloneArgs(args)
	
	entry := &QueryCacheEntry{
		Query:     query,
		Args:      clonedArgs,
		CreatedAt: now,
		LastUsed:  now,
		UseCount:  1,
	}
	
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	// If cache is full, remove least recently used entry
	if len(c.entries) >= c.maxSize {
		c.evictLRU()
	}
	
	c.entries[key] = entry
	return entry
}

// CloneArgs creates a deep copy of the argument slice
// This prevents shared arguments from being modified by other operations
func CloneArgs(args []interface{}) []interface{} {
	if len(args) == 0 {
		return nil
	}
	
	// Get a pre-allocated slice from the pool
	newArgs := argSlicePool.Get().([]interface{})
	newArgs = newArgs[:0] // Reset slice but keep capacity
	
	// Ensure it has enough capacity
	if cap(newArgs) < len(args) {
		// If not enough capacity, create a new slice with double capacity
		newArgs = make([]interface{}, 0, len(args)*2)
	}
	
	// Copy values
	for _, arg := range args {
		newArgs = append(newArgs, arg)
	}
	
	return newArgs
}

// evictLRU removes the least recently used cache entry
func (c *QueryCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time
	
	// Find oldest entry
	for k, v := range c.entries {
		if oldestKey == "" || v.LastUsed.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.LastUsed
		}
	}
	
	// Remove oldest entry
	if oldestKey != "" {
		delete(c.entries, oldestKey)
		atomic.AddInt64(&c.evictions, 1)
	}
}

// Size returns the number of entries in the cache
func (c *QueryCache) Size() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.entries)
}

// Stats returns cache performance statistics
func (c *QueryCache) Stats() map[string]int64 {
	return map[string]int64{
		"size":        int64(c.Size()),
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
		"evictions":   atomic.LoadInt64(&c.evictions),
		"saved_allocs": atomic.LoadInt64(&c.savedAllocations),
	}
}

// Clear empties the cache
func (c *QueryCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.entries = make(map[string]*QueryCacheEntry, c.maxSize)
	c.preparedStmt = make(map[string]*PreparedStatement, c.maxSize)
}

// GetPreparedStatement retrieves a prepared statement from cache
func (c *QueryCache) GetPreparedStatement(queryTemplate string) *PreparedStatement {
	c.mutex.RLock()
	stmt, found := c.preparedStmt[queryTemplate]
	c.mutex.RUnlock()
	
	if !found {
		return nil
	}
	
	// Update usage stats using atomic operations
	atomic.AddInt64(&stmt.UseCount, 1)
	stmt.LastUsed = time.Now()  // Update last used time
	atomic.AddInt64(&c.hits, 1)
	
	return stmt
}

// SetPreparedStatement adds a prepared statement to the cache
func (c *QueryCache) SetPreparedStatement(queryTemplate string, paramIndexes []int) *PreparedStatement {
	now := time.Now()
	
	// Create a copy of parameter indexes to prevent mutation
	indexesCopy := make([]int, len(paramIndexes))
	copy(indexesCopy, paramIndexes)
	
	stmt := &PreparedStatement{
		Query:        queryTemplate,
		ParamIndexes: indexesCopy,
		CreatedAt:    now,
		LastUsed:     now,
		UseCount:     1,
	}
	
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	// If cache is full, remove least recently used entry
	if len(c.preparedStmt) >= c.maxSize {
		c.evictLRUStmt()
	}
	
	c.preparedStmt[queryTemplate] = stmt
	return stmt
}

// evictLRUStmt removes the least recently used prepared statement
func (c *QueryCache) evictLRUStmt() {
	var oldestKey string
	var oldestTime time.Time
	
	// Find oldest entry
	for k, v := range c.preparedStmt {
		if oldestKey == "" || v.LastUsed.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.LastUsed
		}
	}
	
	// Remove oldest entry
	if oldestKey != "" {
		delete(c.preparedStmt, oldestKey)
		atomic.AddInt64(&c.evictions, 1)
	}
}

// CachedQuery retrieves a query from cache or adds it if not found
// This is the main entry point for query caching
func CachedQuery(query string, args []interface{}) (string, []interface{}) {
	// Check if query is in cache
	entry := globalQueryCache.Get(query, args)
	if entry != nil {
		// Return cached query and args
		return entry.Query, entry.Args
	}
	
	// Add query to cache
	entry = globalQueryCache.Set(query, args)
	return entry.Query, entry.Args
}

// GetParamPlaceholders returns parameter placeholders ($1, $2, etc.) for the given count
// This version uses pre-computed placeholders for common sizes
func GetParamPlaceholders(count int) []string {
	if count < len(paramPlaceholders) {
		return paramPlaceholders[count]
	}
	
	// For larger counts, generate on demand
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return placeholders
}

// PrepareQueryTemplate creates a parameterized query template
// This optimized version detects parameter patterns in the query
func PrepareQueryTemplate(query string) (*PreparedStatement, error) {
	// Check if template is already in cache
	stmt := globalQueryCache.GetPreparedStatement(query)
	if stmt != nil {
		return stmt, nil
	}
	
	// Find parameter positions by analyzing the query
	paramPositions := findParamPositions(query)
	
	// Create and cache the prepared statement
	stmt = globalQueryCache.SetPreparedStatement(query, paramPositions)
	return stmt, nil
}

// findParamPositions scans a query for parameter placeholders
// and returns their positions for faster parameter binding
func findParamPositions(query string) []int {
	var positions []int
	inString := false
	escape := false
	
	// Scan the query for parameter placeholders like $1, $2, etc.
	for i := 0; i < len(query); i++ {
		c := query[i]
		
		// Handle string literals
		if c == '\'' && !escape {
			inString = !inString
		}
		
		// Handle escape characters
		if c == '\\' {
			escape = !escape
		} else {
			escape = false
		}
		
		// Only look for parameters outside of string literals
		if !inString && c == '$' && i+1 < len(query) {
			// Check if next character is a digit
			if isDigit(query[i+1]) {
				// Extract the parameter number
				start := i + 1
				end := start
				for end < len(query) && isDigit(query[end]) {
					end++
				}
				
				// Get the parameter number
				paramNum := parseDigits(query[start:end])
				positions = append(positions, paramNum)
			}
		}
	}
	
	return positions
}

// isDigit checks if a character is a digit
func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// parseDigits converts a string of digits to an integer
func parseDigits(s string) int {
	result := 0
	for i := 0; i < len(s); i++ {
		result = result*10 + int(s[i]-'0')
	}
	return result
}

// ResetCache clears all cached queries
func ResetCache() {
	globalQueryCache.Clear()
}