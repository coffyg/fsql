// fsql.go
package fsql

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx" // SQL library
	"github.com/rs/zerolog"
)

// Default connection settings
const (
	defaultMaxConnections      = 20
	defaultMinConnections      = 5
	defaultMaxConnLifetime     = 30 * time.Minute
	defaultMaxConnIdleTime     = 15 * time.Minute
	defaultHealthCheckInterval = 30 * time.Second
	defaultHealthCheckTimeout  = 5 * time.Second
)

// Connection state values
const (
	connStateHealthy int32 = iota
	connStateUnhealthy
	connStateRecovering
)

// DBConnection represents a database connection with health tracking
type DBConnection struct {
	DB           *sqlx.DB
	URI          string
	FailureCount int32
	State        int32
}

// Pool configuration
type DBConfig struct {
	MaxConnections  int
	MinConnections  int
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	HealthCheck     bool
	DefaultTimeout  time.Duration
}

// Global database connections
var (
	Db              *sqlx.DB
	mainPool        *pgxpool.Pool // Keep reference to actual pool for accurate stats
	readReplicasDbs []*DBConnection
	healthCheckStop chan struct{}
	replicaMutex    sync.RWMutex

	// Global logger for fsql operations
	logger *zerolog.Logger

	// Default configuration - reasonable production defaults
	DefaultConfig = DBConfig{
		MaxConnections:  50, // Reasonable default instead of crazy 1200
		MinConnections:  5,
		MaxConnLifetime: 5 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
		HealthCheck:     false,
		DefaultTimeout:  3 * time.Second, // Reasonable timeout for complex queries
	}
)

// SetLogger configures the global logger for fsql operations
func SetLogger(l *zerolog.Logger) {
	logger = l
}

// logQueryTimeout logs when a database query times out
func logQueryTimeout(operation, query string, timeout time.Duration, poolStats ...interface{}) {
	if logger == nil {
		return
	}

	// Extract table name from query for better logging
	tableName := extractTableName(query)

	event := logger.Warn().
		Str("operation", operation).
		Str("table", tableName).
		Dur("timeout", timeout).
		Str("query", query)

	// Add pool stats if available
	if len(poolStats) >= 5 {
		if openConns, ok := poolStats[0].(int32); ok {
			event = event.Int32("pool_open_conns", openConns)
		}
		if inUse, ok := poolStats[1].(int32); ok {
			event = event.Int32("pool_in_use", inUse)
		}
		if idle, ok := poolStats[2].(int32); ok {
			event = event.Int32("pool_idle", idle)
		}
	}

	event.Msg("Database query timed out")
}

// extractTableName attempts to extract table name from SQL query
func extractTableName(query string) string {
	query = strings.ToUpper(strings.TrimSpace(query))

	// Handle INSERT statements
	if strings.HasPrefix(query, "INSERT INTO") {
		parts := strings.Fields(query)
		if len(parts) >= 3 {
			return strings.Trim(parts[2], "\"`)") // Remove quotes/backticks
		}
	}

	// Handle UPDATE statements
	if strings.HasPrefix(query, "UPDATE") {
		parts := strings.Fields(query)
		if len(parts) >= 2 {
			return strings.Trim(parts[1], "\"`)")
		}
	}

	// Handle SELECT statements with FROM
	if strings.Contains(query, "FROM ") {
		fromIndex := strings.Index(query, "FROM ")
		if fromIndex != -1 {
			afterFrom := query[fromIndex+5:]
			parts := strings.Fields(afterFrom)
			if len(parts) >= 1 {
				return strings.Trim(parts[0], "\"`)")
			}
		}
	}

	// Handle DELETE statements
	if strings.HasPrefix(query, "DELETE FROM") {
		parts := strings.Fields(query)
		if len(parts) >= 3 {
			return strings.Trim(parts[2], "\"`)")
		}
	}

	return "unknown"
}

// PgxCreateDBWithPool creates a connection pool with custom configuration
func PgxCreateDBWithPool(uri string, config DBConfig) (*sqlx.DB, error) {
	// Create a connection pool configuration
	poolConfig, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}

	// Apply custom configuration
	poolConfig.MaxConns = int32(config.MaxConnections)
	poolConfig.MinConns = int32(config.MinConnections)
	poolConfig.MaxConnLifetime = config.MaxConnLifetime
	poolConfig.MaxConnIdleTime = config.MaxConnIdleTime

	// Configure connection timeouts to prevent hanging
	poolConfig.ConnConfig.ConnectTimeout = 10 * time.Second // Connection establishment timeout
	poolConfig.HealthCheckPeriod = 30 * time.Second         // Health check interval - reduced for faster recovery
	// pgx v5 pool acquisition timeout is handled via context timeouts in Safe* functions
	// This prevents the 5k connection leak issue by failing fast instead of hanging

	// Create the connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}

	// Store pool reference for accurate stats
	mainPool = pool

	// Wrap pgxpool.Pool as sqlx.DB using stdlib
	db := sqlx.NewDb(stdlib.OpenDBFromPool(pool), "pgx")

	// Let pgxpool handle all connection management - don't set database/sql limits
	// Setting these to 0/-1 can create conflicts with pgxpool's internal management
	// The pgxpool configuration above handles all connection limits and timeouts

	return db, nil
}

// PgxCreateDB creates a simple connection without pooling
func PgxCreateDB(uri string, config ...DBConfig) (*sqlx.DB, error) {
	// Get effective configuration (for potential future use)
	_ = DefaultConfig
	if len(config) > 0 {
		_ = config[0]
	}

	connConfig, err := pgx.ParseConfig(uri)
	if err != nil {
		return nil, err
	}

	pgxdb := stdlib.OpenDB(*connConfig)
	db := sqlx.NewDb(pgxdb, "pgx")

	// Let pgx handle connection management - don't set database/sql limits
	// Setting these can interfere with pgx's connection handling
	// The pgx driver manages connections internally

	return db, nil
}

// InitDBPool initializes the main database pool with custom configuration
func InitDBPool(database string, config ...DBConfig) {
	var err error
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = config[0]
		// Only update global timeout when config is explicitly passed
		DefaultDBTimeout = cfg.DefaultTimeout
	}

	Db, err = PgxCreateDBWithPool(database, cfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}

// InitCustomDb creates a custom database connection
func InitCustomDb(database string) *sqlx.DB {
	db, err := PgxCreateDB(database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	return db
}

// InitDB initializes the main database connection without pooling
func InitDB(database string, config ...DBConfig) {
	// Use default config if none provided
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = config[0]
		// Only update global timeout when config is explicitly passed
		DefaultDBTimeout = cfg.DefaultTimeout
	}

	var err error
	Db, err = PgxCreateDB(database, cfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}

// CloseDB closes the main database connection
func CloseDB() {
	// Stop health check if running
	stopHealthCheck()

	// Clear prepared statement cache
	ClearPreparedCache()

	if Db != nil {
		if err := Db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}
}

// InitDbReplicas initializes read replica connections
func InitDbReplicas(databases []string, config ...DBConfig) {
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	// Lock for writing to replicas
	replicaMutex.Lock()
	defer replicaMutex.Unlock()

	// Initialize slice with capacity
	readReplicasDbs = make([]*DBConnection, 0, len(databases))

	// Create connections for each replica
	for _, dbURI := range databases {
		var replicaDb *sqlx.DB
		var err error

		// Create replica with pool if configured
		if cfg.MaxConnections > 0 {
			replicaDb, err = PgxCreateDBWithPool(dbURI, cfg)
		} else {
			replicaDb, err = PgxCreateDB(dbURI)
		}

		if err != nil {
			log.Printf("Failed to connect to replica database %s: %v", dbURI, err)
			continue
		}

		// Add to replicas list
		readReplicasDbs = append(readReplicasDbs, &DBConnection{
			DB:           replicaDb,
			URI:          dbURI,
			FailureCount: 0,
			State:        connStateHealthy,
		})
	}

	// Start health check if enabled and there are replicas
	if cfg.HealthCheck && len(readReplicasDbs) > 0 {
		startHealthCheck()
	}
}

// GetReplika returns a read replica connection using load balancing
func GetReplika() *sqlx.DB {
	replicaMutex.RLock()
	defer replicaMutex.RUnlock()

	if len(readReplicasDbs) == 0 {
		return Db
	}

	// Count healthy replicas
	var healthyReplicas []*DBConnection
	for _, replica := range readReplicasDbs {
		if atomic.LoadInt32(&replica.State) == connStateHealthy {
			healthyReplicas = append(healthyReplicas, replica)
		}
	}

	// If no healthy replicas, return primary
	if len(healthyReplicas) == 0 {
		return Db
	}

	// Select a random healthy replica
	idx := rand.Intn(len(healthyReplicas))
	return healthyReplicas[idx].DB
}

// CloseReplicas closes all replica connections
func CloseReplicas() {
	// Stop health check if running
	stopHealthCheck()

	replicaMutex.Lock()
	defer replicaMutex.Unlock()

	for _, conn := range readReplicasDbs {
		if conn.DB != nil {
			if err := conn.DB.Close(); err != nil {
				log.Printf("Error closing replica database: %v", err)
			}
		}
	}

	// Clear replicas list
	readReplicasDbs = nil
}

// Start health check for replicas
func startHealthCheck() {
	// If already running, stop it first
	stopHealthCheck()

	// Create stop channel
	healthCheckStop = make(chan struct{})

	// Start health check goroutine
	go func() {
		ticker := time.NewTicker(defaultHealthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Check health of all replicas
				checkReplicasHealth()
			case <-healthCheckStop:
				return
			}
		}
	}()
}

// Stop health check
func stopHealthCheck() {
	if healthCheckStop != nil {
		close(healthCheckStop)
		healthCheckStop = nil
	}
}

// Check health of all replicas
func checkReplicasHealth() {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), defaultHealthCheckTimeout)
	defer cancel()

	// Create wait group for parallel health checks
	var wg sync.WaitGroup

	// Lock for reading replicas
	replicaMutex.RLock()
	replicas := readReplicasDbs // Copy to avoid holding lock
	replicaMutex.RUnlock()

	for _, replica := range replicas {
		wg.Add(1)

		// Run health check in goroutine
		go func(conn *DBConnection) {
			defer wg.Done()

			// Get current state
			currentState := atomic.LoadInt32(&conn.State)

			// Check connection health
			err := conn.DB.PingContext(ctx)

			if err != nil {
				// Increment failure count
				failCount := atomic.AddInt32(&conn.FailureCount, 1)

				// If connection is healthy, mark it as unhealthy after first failure
				if currentState == connStateHealthy && failCount >= 1 {
					atomic.StoreInt32(&conn.State, connStateUnhealthy)
					log.Printf("Replica %s is unhealthy: %v", conn.URI, err)
				}

				// If connection is recovering, reset to unhealthy if it fails again
				if currentState == connStateRecovering {
					atomic.StoreInt32(&conn.State, connStateUnhealthy)
					log.Printf("Replica %s failed recovery check: %v", conn.URI, err)
				}
			} else {
				// Connection is healthy

				// If connection was unhealthy, mark it as recovering
				if currentState == connStateUnhealthy {
					atomic.StoreInt32(&conn.State, connStateRecovering)
					log.Printf("Replica %s is recovering", conn.URI)
				}

				// If connection was recovering, mark it as healthy after success
				if currentState == connStateRecovering {
					atomic.StoreInt32(&conn.State, connStateHealthy)
					atomic.StoreInt32(&conn.FailureCount, 0)
					log.Printf("Replica %s is now healthy", conn.URI)
				}

				// Reset failure count for healthy connections
				if currentState == connStateHealthy {
					atomic.StoreInt32(&conn.FailureCount, 0)
				}
			}
		}(replica)
	}

	// Wait for all health checks to complete
	wg.Wait()
}

// IsConnectionHealthy checks if a specific connection is healthy
func IsConnectionHealthy(db *sqlx.DB) bool {
	if db == nil {
		return false
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), defaultHealthCheckTimeout)
	defer cancel()

	// Ping the database
	err := db.PingContext(ctx)
	return err == nil
}

// ExecuteWithRetry executes a query with retry logic
func ExecuteWithRetry(query string, args ...interface{}) error {
	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		_, err := Db.Exec(query, args...)
		if err != nil {
			lastErr = err
			// Exponential backoff
			time.Sleep(time.Duration(1<<uint(i)) * 100 * time.Millisecond)
			continue
		}
		return nil
	}

	if lastErr != nil {
		return errors.New("max retries exceeded: " + lastErr.Error())
	}
	return errors.New("max retries exceeded with unknown error")
}

// Default timeout for database operations
var DefaultDBTimeout = 30 * time.Second

// Timeout wrapper functions - these add automatic timeouts to existing context-less calls
// This provides immediate protection for legacy code without requiring refactoring

var (
	dbTimeoutWarningLogged bool
)

// SafeExec wraps Db.Exec with automatic timeout
func SafeExec(query string, args ...interface{}) (sql.Result, error) {
	return SafeExecTimeout(DefaultDBTimeout, query, args...)
}

// SafeExecTimeout wraps Db.Exec with custom timeout
func SafeExecTimeout(timeout time.Duration, query string, args ...interface{}) (sql.Result, error) {
	// Determine operation name based on whether this is default or custom timeout
	operationName := "SafeExecTimeout"

	// Check if this was called through SafeExec (default timeout)
	if timeout == DefaultDBTimeout {
		operationName = "SafeExec" // This was called through SafeExec
	}

	// Show warning for first Safe wrapper usage with method name and timeout
	if !dbTimeoutWarningLogged && logger != nil {
		logger.Warn().
			Str("operation", operationName).
			Str("query", query).
			Dur("timeout", timeout).
			Msg("Using Safe wrapper with timeout protection")
		dbTimeoutWarningLogged = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	startTime := time.Now()
	result, err := Db.ExecContext(ctx, query, args...)
	actualDuration := time.Since(startTime)

	// Enhanced error detection and logging for connection vs query timeouts
	
	// Log any context cancellation (timeout, cancellation, etc) with correct operation name
	if err != nil && ctx.Err() != nil {
		openConns, inUse, idle, waitCount, waitDuration := GetPoolStats()
		
		// Detect likely pool exhaustion vs query timeout
		isPoolExhausted := (inUse >= openConns) && (idle == 0) && (waitCount > 0)
		timeoutType := "query_timeout"
		if isPoolExhausted && actualDuration < timeout/2 {
			timeoutType = "pool_acquisition_timeout"
		}
		
		if logger != nil {
			logger.Error().
				Str("operation", operationName).
				Str("timeout_type", timeoutType).
				Dur("timeout_requested", timeout).
				Dur("actual_duration", actualDuration).
				Bool("pool_exhausted", isPoolExhausted).
				Int32("pool_open", openConns).
				Int32("pool_in_use", inUse).
				Int32("pool_idle", idle).
				Int64("pool_wait_count", waitCount).
				Dur("pool_wait_duration", waitDuration).
				Err(ctx.Err()).
				Msg("Database operation failed with context error")
		}
		logQueryTimeout(operationName, query, timeout, openConns, inUse, idle, waitCount, waitDuration)
	}

	return result, err
}

// SafeQuery wraps Db.Query with automatic timeout
func SafeQuery(query string, args ...interface{}) (*sql.Rows, error) {
	return SafeQueryTimeout(DefaultDBTimeout, query, args...)
}

// SafeQueryTimeout wraps Db.Query with custom timeout
// Note: Uses timeout context for query execution but allows row scanning after context expires
func SafeQueryTimeout(timeout time.Duration, query string, args ...interface{}) (*sql.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	// Let context expire naturally - DON'T cancel immediately as it breaks row scanning
	
	rows, err := Db.QueryContext(ctx, query, args...)
	
	// Log context cancellation issues for debugging
	if err != nil && ctx.Err() != nil {
		openConns, inUse, idle, waitCount, waitDuration := GetPoolStats()
		logQueryTimeout("SafeQueryTimeout", query, timeout, openConns, inUse, idle, waitCount, waitDuration)
	}
	
	// Schedule context cancellation after a delay to allow row scanning
	// This prevents resource leaks while allowing proper row consumption
	go func() {
		time.Sleep(timeout + 5*time.Second) // Give extra time for row scanning
		cancel()
	}()
	
	return rows, err
}

// SafeGet wraps Db.Get with automatic timeout
func SafeGet(dest interface{}, query string, args ...interface{}) error {
	return SafeGetTimeout(DefaultDBTimeout, dest, query, args...)
}

// SafeGetTimeout wraps Db.Get with custom timeout
func SafeGetTimeout(timeout time.Duration, dest interface{}, query string, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := Db.GetContext(ctx, dest, query, args...)

	// Log any context cancellation (timeout, cancellation, etc)
	if err != nil && ctx.Err() != nil {
		openConns, inUse, idle, waitCount, waitDuration := GetPoolStats()
		logQueryTimeout("SafeGetTimeout", query, timeout, openConns, inUse, idle, waitCount, waitDuration)
	}

	return err
}

// SafeSelect wraps Db.Select with automatic timeout
func SafeSelect(dest interface{}, query string, args ...interface{}) error {
	return SafeSelectTimeout(DefaultDBTimeout, dest, query, args...)
}

// SafeSelectTimeout wraps Db.Select with custom timeout
func SafeSelectTimeout(timeout time.Duration, dest interface{}, query string, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := Db.SelectContext(ctx, dest, query, args...)

	// Log any context cancellation (timeout, cancellation, etc)
	if err != nil && ctx.Err() != nil {
		openConns, inUse, idle, waitCount, waitDuration := GetPoolStats()
		logQueryTimeout("SafeSelectTimeout", query, timeout, openConns, inUse, idle, waitCount, waitDuration)
	}

	return err
}

// SafeQueryRow wraps Db.QueryRow with automatic timeout
func SafeQueryRow(query string, args ...interface{}) *sql.Row {
	return SafeQueryRowTimeout(DefaultDBTimeout, query, args...)
}

// SafeQueryRowTimeout wraps Db.QueryRow with custom timeout
// Note: Uses timeout context for query execution but allows row scanning after context expires
func SafeQueryRowTimeout(timeout time.Duration, query string, args ...interface{}) *sql.Row {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	// Let context expire naturally - DON'T cancel immediately as it breaks row scanning
	
	row := Db.QueryRowContext(ctx, query, args...)
	
	// Note: QueryRow doesn't return error directly, but we can check context state
	if ctx.Err() != nil {
		openConns, inUse, idle, waitCount, waitDuration := GetPoolStats()
		logQueryTimeout("SafeQueryRowTimeout", query, timeout, openConns, inUse, idle, waitCount, waitDuration)
	}
	
	// Schedule context cancellation after a delay to allow row scanning
	go func() {
		time.Sleep(timeout + 5*time.Second) // Give extra time for row scanning
		cancel()
	}()
	
	return row
}

// SafeNamedExec wraps Db.NamedExec with automatic timeout
func SafeNamedExec(query string, arg interface{}) (sql.Result, error) {
	return SafeNamedExecTimeout(DefaultDBTimeout, query, arg)
}

// SafeNamedExecTimeout wraps Db.NamedExec with custom timeout
func SafeNamedExecTimeout(timeout time.Duration, query string, arg interface{}) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := Db.NamedExecContext(ctx, query, arg)

	// Log any context cancellation (timeout, cancellation, etc)
	if err != nil && ctx.Err() != nil {
		openConns, inUse, idle, waitCount, waitDuration := GetPoolStats()
		logQueryTimeout("SafeNamedExecTimeout", query, timeout, openConns, inUse, idle, waitCount, waitDuration)
	}

	return result, err
}

// SafeNamedQuery wraps Db.NamedQuery with automatic timeout
func SafeNamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	return SafeNamedQueryTimeout(DefaultDBTimeout, query, arg)
}

// SafeNamedQueryTimeout wraps Db.NamedQuery with custom timeout
// Note: Uses timeout context for query execution but allows row scanning after context expires
func SafeNamedQueryTimeout(timeout time.Duration, query string, arg interface{}) (*sqlx.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	// Let context expire naturally - DON'T cancel immediately as it breaks row scanning
	
	rows, err := Db.NamedQueryContext(ctx, query, arg)
	
	// Log context cancellation issues for debugging
	if err != nil && ctx.Err() != nil {
		openConns, inUse, idle, waitCount, waitDuration := GetPoolStats()
		logQueryTimeout("SafeNamedQueryTimeout", query, timeout, openConns, inUse, idle, waitCount, waitDuration)
	}
	
	// Schedule context cancellation after a delay to allow row scanning
	go func() {
		time.Sleep(timeout + 5*time.Second) // Give extra time for row scanning
		cancel()
	}()
	
	return rows, err
}

// SafeBegin wraps Db.BeginTx (no timeout for transaction creation - transactions are long-lived)
func SafeBegin() (*sql.Tx, error) {
	// Transactions should not have timeout on creation since they live beyond function scope
	return Db.BeginTx(context.Background(), nil)
}

// SafeBeginx wraps Db.BeginTxx (no timeout for transaction creation - transactions are long-lived)
func SafeBeginx() (*sqlx.Tx, error) {
	// Transactions should not have timeout on creation since they live beyond function scope
	return Db.BeginTxx(context.Background(), nil)
}

// GetPoolStats returns accurate connection pool statistics
// Returns pgxpool stats if available, falls back to database/sql stats otherwise
func GetPoolStats() (openConns, inUse, idle int32, waitCount int64, waitDuration time.Duration) {
	if mainPool != nil {
		// Get accurate stats from pgxpool
		stats := mainPool.Stat()
		openConns = stats.TotalConns()
		inUse = stats.AcquiredConns()
		idle = stats.IdleConns()
		waitCount = stats.EmptyAcquireCount()
		waitDuration = stats.EmptyAcquireWaitTime()
		
		// Detect potential connection leaks - warn if connections exceed configured limits significantly
		if logger != nil && openConns > int32(DefaultConfig.MaxConnections*2) {
			logger.Warn().
				Int32("total_conns", openConns).
				Int32("acquired_conns", inUse).
				Int32("idle_conns", idle).
				Int64("empty_acquire_count", waitCount).
				Dur("empty_acquire_wait_time", waitDuration).
				Int("configured_max", DefaultConfig.MaxConnections).
				Msg("Potential connection leak detected - connections exceed configured maximum")
		}
		
		return openConns, inUse, idle, waitCount, waitDuration
	} else {
		// Fall back to database/sql stats (less accurate with our setup)
		sqlStats := Db.Stats()
		return int32(sqlStats.OpenConnections), int32(sqlStats.InUse), int32(sqlStats.Idle),
			sqlStats.WaitCount, sqlStats.WaitDuration
	}
}
