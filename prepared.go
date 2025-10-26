package fsql

import (
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/jmoiron/sqlx"
)

// PreparedStmt represents a prepared statement with usage statistics
type PreparedStmt struct {
	// Statement object
	stmt *sqlx.Stmt
	
	// Usage statistics
	useCount int64
	lastUsed time.Time
	isSelect bool
	
	// Query information
	query     string
	paramCount int
}

// PreparedStmtCache manages a cache of prepared statements
type PreparedStmtCache struct {
	// Cache of statements by query text
	statements map[string]*PreparedStmt
	
	// Statistics
	hits   int64
	misses int64
	
	// Cache configuration
	maxStatements int
	ttl           time.Duration
	
	// Mutex for thread safety
	mu sync.RWMutex
}

// GlobalStmtCache is the global prepared statement cache
var GlobalStmtCache = NewPreparedStmtCache(100, 30*time.Minute)

// NewPreparedStmtCache creates a new prepared statement cache
func NewPreparedStmtCache(maxStatements int, ttl time.Duration) *PreparedStmtCache {
	return &PreparedStmtCache{
		statements:   make(map[string]*PreparedStmt, maxStatements),
		maxStatements: maxStatements,
		ttl:           ttl,
	}
}

// Get retrieves a prepared statement from the cache
func (c *PreparedStmtCache) Get(query string) *PreparedStmt {
	// Fast path: check if statement is in cache
	c.mu.RLock()
	stmt, ok := c.statements[query]
	c.mu.RUnlock()
	
	if ok {
		atomic.AddInt64(&c.hits, 1)
		atomic.AddInt64(&stmt.useCount, 1)
		return stmt
	}
	
	// Statement not found
	atomic.AddInt64(&c.misses, 1)
	return nil
}

// Add adds a prepared statement to the cache
func (c *PreparedStmtCache) Add(query string, stmt *sqlx.Stmt, isSelect bool, paramCount int) *PreparedStmt {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Evict old statements if cache is full
	if len(c.statements) >= c.maxStatements {
		c.evictLRU()
	}
	
	// Create new entry
	ps := &PreparedStmt{
		stmt:       stmt,
		useCount:   1,
		lastUsed:   time.Now(),
		isSelect:   isSelect,
		query:      query,
		paramCount: paramCount,
	}
	
	c.statements[query] = ps
	return ps
}

// evictLRU removes the least recently used statement
func (c *PreparedStmtCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time
	
	for k, v := range c.statements {
		if oldestKey == "" || v.lastUsed.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.lastUsed
		}
	}
	
	if oldestKey != "" {
		// Close the statement synchronously
		stmt := c.statements[oldestKey]
		stmt.stmt.Close()

		// Remove from cache
		delete(c.statements, oldestKey)
	}
}

// Clear closes all statements and clears the cache
func (c *PreparedStmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Close all statements
	for _, stmt := range c.statements {
		stmt.stmt.Close()
	}
	
	// Clear the map
	c.statements = make(map[string]*PreparedStmt, c.maxStatements)
}

// Stats returns cache statistics
func (c *PreparedStmtCache) Stats() map[string]int64 {
	c.mu.RLock()
	stmtCount := len(c.statements)
	c.mu.RUnlock()
	
	return map[string]int64{
		"size":   int64(stmtCount),
		"hits":   atomic.LoadInt64(&c.hits),
		"misses": atomic.LoadInt64(&c.misses),
	}
}

// GetPreparedStmt retrieves a prepared statement or creates one if not found
func GetPreparedStmt(query string, isSelect bool) (*sqlx.Stmt, error) {
	// Check if we have a cached prepared statement
	cached := GlobalStmtCache.Get(query)
	if cached != nil {
		cached.lastUsed = time.Now()
		return cached.stmt, nil
	}
	
	// Prepare the statement
	var stmt *sqlx.Stmt
	var err error
	
	if isSelect {
		stmt, err = Db.Preparex(query)
	} else {
		stmt, err = Db.Preparex(query)
	}
	
	if err != nil {
		return nil, err
	}
	
	// Count parameters in the query
	paramCount := countParameters(query)
	
	// Add to cache
	GlobalStmtCache.Add(query, stmt, isSelect, paramCount)
	
	return stmt, nil
}

// countParameters counts the number of parameters in a query
func countParameters(query string) int {
	count := 0
	inString := false
	escape := false
	
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
		
		// Only count parameters outside of string literals
		if !inString && c == '$' && i+1 < len(query) {
			// Check if next character is a digit
			if i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
				count++
			}
		}
	}
	
	return count
}

// ExecuteWithPrepared executes a query using a prepared statement
func ExecuteWithPrepared(query string, args []interface{}, result interface{}) error {
	// Get or create the prepared statement
	stmt, err := GetPreparedStmt(query, true)
	if err != nil {
		return err
	}
	
	// Use optimized scanner for better performance
	rows, err := stmt.Queryx(args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	
	// Use the optimized scanner
	return ScanRows(rows, result)
}

// InitPreparedCache initializes the prepared statement cache
func InitPreparedCache(maxStatements int, ttl time.Duration) {
	GlobalStmtCache = NewPreparedStmtCache(maxStatements, ttl)
}

// ClearPreparedCache clears the prepared statement cache
func ClearPreparedCache() {
	GlobalStmtCache.Clear()
}