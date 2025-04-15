// fsql.go
package fsql

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx" // SQL library
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
}

// Global database connections
var (
	Db             *sqlx.DB
	readReplicasDbs []*DBConnection
	healthCheckStop chan struct{}
	replicaMutex    sync.RWMutex
	
	// Default configuration
	DefaultConfig = DBConfig{
		MaxConnections:  defaultMaxConnections,
		MinConnections:  defaultMinConnections,
		MaxConnLifetime: defaultMaxConnLifetime,
		MaxConnIdleTime: defaultMaxConnIdleTime,
		HealthCheck:     true,
	}
)

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
	
	// Create the connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}

	// Wrap pgxpool.Pool as sqlx.DB using stdlib
	return sqlx.NewDb(stdlib.OpenDBFromPool(pool), "pgx"), nil
}

// PgxCreateDB creates a simple connection without pooling
func PgxCreateDB(uri string) (*sqlx.DB, error) {
	connConfig, err := pgx.ParseConfig(uri)
	if err != nil {
		return nil, err
	}

	pgxdb := stdlib.OpenDB(*connConfig)
	return sqlx.NewDb(pgxdb, "pgx"), nil
}

// InitDBPool initializes the main database pool with custom configuration
func InitDBPool(database string, config ...DBConfig) {
	var err error
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = config[0]
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
func InitDB(database string) {
	var err error
	Db, err = PgxCreateDB(database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	
	// Initialize prepared statement cache
	InitPreparedCache(100, 30*time.Minute)
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
