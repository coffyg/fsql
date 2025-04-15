// transaction_benchmark_test.go
package fsql

import (
	"context"
	"database/sql"
	"testing"
)

func setupTransactionBenchmark(b *testing.B) {
	// Create the test table if it doesn't exist
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS transaction_benchmark (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)
	if err != nil {
		b.Fatalf("failed to create test table: %v", err)
	}
	
	// Clean the table before each benchmark
	_, err = Db.Exec("TRUNCATE transaction_benchmark")
	if err != nil {
		b.Fatalf("failed to truncate test table: %v", err)
	}
}

// BenchmarkTransactionSimple measures simple transaction performance
func BenchmarkTransactionSimple(b *testing.B) {
	setupTransactionBenchmark(b)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := Db.Beginx()
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}
		
		_, err = tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
			"test", i)
		if err != nil {
			tx.Rollback()
			b.Fatalf("failed to insert: %v", err)
		}
		
		err = tx.Commit()
		if err != nil {
			b.Fatalf("failed to commit: %v", err)
		}
	}
}

// BenchmarkTransactionOptimized measures optimized transaction performance
func BenchmarkTransactionOptimized(b *testing.B) {
	setupTransactionBenchmark(b)
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := BeginTx(ctx)
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}
		
		_, err = tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
			"test", i)
		if err != nil {
			tx.Rollback()
			b.Fatalf("failed to insert: %v", err)
		}
		
		err = tx.Commit()
		if err != nil {
			b.Fatalf("failed to commit: %v", err)
		}
	}
}

// BenchmarkTransactionWithTx measures WithTx helper performance
func BenchmarkTransactionWithTx(b *testing.B) {
	setupTransactionBenchmark(b)
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := WithTx(ctx, func(tx *Tx) error {
			_, err := tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
				"test", i)
			return err
		})
		if err != nil {
			b.Fatalf("transaction failed: %v", err)
		}
	}
}

// BenchmarkTransactionMultiOp measures multiple operations in a transaction
func BenchmarkTransactionMultiOp(b *testing.B) {
	setupTransactionBenchmark(b)
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := WithTx(ctx, func(tx *Tx) error {
			// First insert
			_, err := tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
				"test1", i)
			if err != nil {
				return err
			}
			
			// Second insert
			_, err = tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
				"test2", i+1)
			if err != nil {
				return err
			}
			
			// Query
			var count int
			err = tx.Get(&count, "SELECT COUNT(*) FROM transaction_benchmark WHERE value >= $1", i)
			if err != nil {
				return err
			}
			
			return nil
		})
		if err != nil {
			b.Fatalf("transaction failed: %v", err)
		}
	}
}

// BenchmarkTransactionGetSelect measures Get and Select in transactions
func BenchmarkTransactionGetSelect(b *testing.B) {
	setupTransactionBenchmark(b)
	ctx := context.Background()
	
	// Populate the table with test data
	for i := 0; i < 100; i++ {
		_, err := Db.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
			"test", i)
		if err != nil {
			b.Fatalf("failed to insert test data: %v", err)
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := WithTx(ctx, func(tx *Tx) error {
			// Get a single record
			var record struct {
				ID    int    `db:"id"`
				Name  string `db:"name"`
				Value int    `db:"value"`
			}
			err := tx.Get(&record, "SELECT id, name, value FROM transaction_benchmark WHERE id = $1", 1)
			if err != nil && err != sql.ErrNoRows {
				return err
			}
			
			// Select multiple records
			var records []struct {
				ID    int    `db:"id"`
				Name  string `db:"name"`
				Value int    `db:"value"`
			}
			err = tx.Select(&records, "SELECT id, name, value FROM transaction_benchmark WHERE value < $1 LIMIT 10", 50)
			if err != nil {
				return err
			}
			
			return nil
		})
		if err != nil {
			b.Fatalf("transaction failed: %v", err)
		}
	}
}

// BenchmarkTransactionIsolation measures transaction isolation performance
func BenchmarkTransactionIsolation(b *testing.B) {
	setupTransactionBenchmark(b)
	ctx := context.Background()
	
	isolationLevels := []struct {
		name  string
		level sql.IsolationLevel
	}{
		{"ReadCommitted", sql.LevelReadCommitted},
		{"RepeatableRead", sql.LevelRepeatableRead},
		{"Serializable", sql.LevelSerializable},
	}
	
	for _, iso := range isolationLevels {
		b.Run(iso.name, func(b *testing.B) {
			opts := TxOptions{
				Isolation: iso.level,
			}
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := WithTxOptions(ctx, opts, func(tx *Tx) error {
					_, err := tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
						iso.name, i)
					return err
				})
				if err != nil {
					b.Fatalf("transaction failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkTransactionRetry measures transaction retry performance
func BenchmarkTransactionRetry(b *testing.B) {
	setupTransactionBenchmark(b)
	ctx := context.Background()
	
	// This is a synthetic benchmark that doesn't actually retry
	// but measures the overhead of the retry mechanism
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := WithTxRetry(ctx, func(tx *Tx) error {
			_, err := tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
				"retry-test", i)
			return err
		})
		if err != nil {
			b.Fatalf("transaction failed: %v", err)
		}
	}
}

// BenchmarkTransactionPrepared measures prepared statement performance in transactions
func BenchmarkTransactionPrepared(b *testing.B) {
	setupTransactionBenchmark(b)
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := WithTx(ctx, func(tx *Tx) error {
			// The Exec method in our Tx implementation uses prepared statements
			_, err := tx.Exec("INSERT INTO transaction_benchmark (name, value) VALUES ($1, $2)", 
				"prepared-test", i)
			return err
		})
		if err != nil {
			b.Fatalf("transaction failed: %v", err)
		}
	}
}