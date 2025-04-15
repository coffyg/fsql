package fsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/coffyg/octypes"
)

// Benchmark for comparing standard vs. optimized join query execution
func BenchmarkOptimizedJoinQuery(b *testing.B) {
	// Skip if not connected to DB
	if Db == nil {
		b.Skip("Database not connected, skipping test")
	}
	
	// Clean the database before the benchmark
	if err := cleanDatabase(); err != nil {
		b.Fatalf("Failed to clean database: %v", err)
	}

	// Reset the cache
	ResetCache()
	
	// Create test data
	// Insert multiple Realms
	realm1 := RealmTest{
		UUID:      GenNewUUID(""),
		Name:      "Realm One",
		CreatedAt: octypes.NewCustomTime(time.Now()),
		UpdatedAt: octypes.NewCustomTime(time.Now()),
	}
	realm2 := RealmTest{
		UUID:      GenNewUUID(""),
		Name:      "Realm Two",
		CreatedAt: octypes.NewCustomTime(time.Now()),
		UpdatedAt: octypes.NewCustomTime(time.Now()),
	}
	
	// Insert realms
	insertRealmBench(b, realm1)
	insertRealmBench(b, realm2)

	// Insert multiple websites for each realm
	for i := 1; i <= 50; i++ {
		website := WebsiteTest{
			UUID:      GenNewUUID(""),
			Domain:    fmt.Sprintf("site%d.com", i),
			RealmUUID: realm1.UUID,
			CreatedAt: *octypes.NewCustomTime(time.Now()),
			UpdatedAt: *octypes.NewCustomTime(time.Now()),
		}
		insertWebsiteBench(b, website)
	}

	for i := 51; i <= 100; i++ {
		website := WebsiteTest{
			UUID:      GenNewUUID(""),
			Domain:    fmt.Sprintf("site%d.com", i),
			RealmUUID: realm2.UUID,
			CreatedAt: *octypes.NewCustomTime(time.Now()),
			UpdatedAt: *octypes.NewCustomTime(time.Now()),
		}
		insertWebsiteBench(b, website)
	}
	
	// Build a query with WHERE and JOIN clauses
	qb := SelectBase("website", "").
		Left("realm", "r", "website.realm_uuid = r.uuid").
		Where("r.name = $1").
		Where("website.domain LIKE $2")
	query := qb.Build()
	args := []interface{}{"Realm One", "%com"}
	
	// Compare standard vs optimized execution
	b.Run("StandardExecution", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			websites := []WebsiteTest{}
			err := Db.Select(&websites, query, args...)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			if len(websites) != 50 {
				b.Fatalf("Expected 50 websites, got %d", len(websites))
			}
		}
	})
	
	// Reset the cache between runs
	ResetCache()
	
	b.Run("OptimizedExecution", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			websites := []WebsiteTest{}
			err := ExecuteJoinQueryOptimized(query, args, &websites)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			if len(websites) != 50 {
				b.Fatalf("Expected 50 websites, got %d", len(websites))
			}
		}
	})
	
	// Add a new benchmark for the fully optimized execution
	b.Run("FullyOptimizedExecution", func(b *testing.B) {
		// Pre-allocate the result slice to reduce allocations
		websites := make([]WebsiteTest, 0, 50)
		
		// Pre-prepare the statement
		stmt, err := PrepareQueryTemplate(query)
		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Reset the slice but maintain capacity
			websites = websites[:0]
			
			// Use our fully optimized execution path
			err := ExecuteJoinQueryOptimized(stmt.Query, args, &websites)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			if len(websites) != 50 {
				b.Fatalf("Expected 50 websites, got %d", len(websites))
			}
		}
	})
	
	// Add a benchmark for prepared statement execution
	b.Run("PreparedStatementExecution", func(b *testing.B) {
		// Pre-allocate the result slice to reduce allocations
		websites := make([]WebsiteTest, 0, 50)
		
		// Set up the prepared statement directly
		_, err := GetPreparedStmt(query, true)
		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Reset the slice but maintain capacity
			websites = websites[:0]
			
			// Use direct prepared statement execution
			err := ExecuteWithPrepared(query, args, &websites)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			if len(websites) != 50 {
				b.Fatalf("Expected 50 websites, got %d", len(websites))
			}
		}
	})
}

// Benchmark for optimized prepared statement
func BenchmarkPreparedJoinQuery(b *testing.B) {
	// Skip if not connected to DB
	if Db == nil {
		b.Skip("Database not connected, skipping test")
	}
	
	// Build a query with WHERE and JOIN clauses
	qb := SelectBase("website", "").
		Left("realm", "r", "website.realm_uuid = r.uuid").
		Where("r.name = $1").
		Where("website.domain LIKE $2")
	query := qb.Build()
	
	// Create a prepared statement (this prepares once, outside the benchmark loop)
	stmt, err := PrepareQueryTemplate(query)
	if err != nil {
		b.Fatalf("Failed to prepare statement: %v", err)
	}
	
	args := []interface{}{"Realm One", "%com"}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		websites := []WebsiteTest{}
		// Use the cached version for execution
		err := ExecuteJoinQueryWithCache(stmt.Query, args, &websites)
		if err != nil {
			b.Fatalf("Failed to execute prepared query: %v", err)
		}
		if len(websites) != 50 {
			b.Fatalf("Expected 50 websites, got %d", len(websites))
		}
	}
}

// Benchmark for query caching
func BenchmarkQueryCaching(b *testing.B) {
	// Skip if not connected to DB
	if Db == nil {
		b.Skip("Database not connected, skipping test")
	}
	
	// Clean the cache
	ResetCache()
	
	// Build a query with WHERE and JOIN clauses
	qb := SelectBase("website", "").
		Left("realm", "r", "website.realm_uuid = r.uuid").
		Where("r.name = $1").
		Where("website.domain LIKE $2")
	query := qb.Build()
	args := []interface{}{"Realm One", "%com"}
	
	b.ResetTimer()
	
	// First run - should populate the cache
	b.Run("FirstRun", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cachedQuery, cachedArgs := CachedQuery(query, args)
			websites := []WebsiteTest{}
			err := Db.Select(&websites, cachedQuery, cachedArgs...)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
		}
	})
	
	// Second run - should use the cache
	b.Run("CachedRun", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cachedQuery, cachedArgs := CachedQuery(query, args)
			websites := []WebsiteTest{}
			err := Db.Select(&websites, cachedQuery, cachedArgs...)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
		}
	})
}