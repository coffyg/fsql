// transaction_test.go
package fsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestTransactionCommit tests a basic transaction commit
func TestTransactionCommit(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Start a transaction
	ctx := context.Background()
	tx, err := BeginTx(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Insert a record
	_, err = tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "test1", 100)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert record: %v", err)
	}
	
	// Insert another record
	_, err = tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "test2", 200)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert second record: %v", err)
	}
	
	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	
	// Verify the records were inserted
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	
	if count != 2 {
		t.Errorf("Expected 2 records, got %d", count)
	}
}

// TestTransactionRollback tests a transaction rollback
func TestTransactionRollback(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Start a transaction
	ctx := context.Background()
	tx, err := BeginTx(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Insert a record
	_, err = tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "test1", 100)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert record: %v", err)
	}
	
	// Rollback the transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
	
	// Verify the record was not inserted
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	
	if count != 0 {
		t.Errorf("Expected 0 records, got %d", count)
	}
}

// TestWithTx tests the WithTx helper
func TestWithTx(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Use WithTx helper
	ctx := context.Background()
	err = WithTx(ctx, func(tx *Tx) error {
		// Insert a record
		_, err := tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "test1", 100)
		if err != nil {
			return err
		}
		
		// Insert another record
		_, err = tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "test2", 200)
		if err != nil {
			return err
		}
		
		return nil
	})
	
	if err != nil {
		t.Fatalf("WithTx failed: %v", err)
	}
	
	// Verify the records were inserted
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	
	if count != 2 {
		t.Errorf("Expected 2 records, got %d", count)
	}
}

// TestWithTxRollback tests the WithTx helper with a rollback
func TestWithTxRollback(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Use WithTx helper with an error to cause rollback
	ctx := context.Background()
	err = WithTx(ctx, func(tx *Tx) error {
		// Insert a record
		_, err := tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "test1", 100)
		if err != nil {
			return err
		}
		
		// Return an error to cause rollback
		return errors.New("test error")
	})
	
	if err == nil {
		t.Fatalf("Expected error from WithTx, got nil")
	}
	
	// Verify the record was not inserted
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	
	if count != 0 {
		t.Errorf("Expected 0 records, got %d", count)
	}
}

// TestWithTxRetry tests the WithTxRetry helper
func TestWithTxRetry(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Use WithTxRetry helper
	ctx := context.Background()
	
	// Mock a function that fails once then succeeds
	attemptCount := 0
	err = WithTxRetry(ctx, func(tx *Tx) error {
		// Insert a record
		_, err := tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "test1", 100)
		if err != nil {
			return err
		}
		
		// Fail on the first attempt
		attemptCount++
		if attemptCount == 1 {
			return errors.New("deadlock detected")
		}
		
		return nil
	})
	
	if err != nil {
		t.Fatalf("WithTxRetry failed: %v", err)
	}
	
	// Verify the record was inserted
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
	
	if attemptCount != 2 {
		t.Errorf("Expected 2 attempts, got %d", attemptCount)
	}
}

// TestTransactionQueriesAndGetSelect tests query, get and select operations in a transaction
func TestTransactionQueriesAndGetSelect(t *testing.T) {
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Insert test data
	_, err = Db.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2), ($3, $4), ($5, $6)",
		"test1", 100, "test2", 200, "test3", 300)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
	
	// Start a transaction
	ctx := context.Background()
	tx, err := BeginTx(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Test Query
	rows, err := tx.Query("SELECT id, name, value FROM tx_test ORDER BY id")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to query: %v", err)
	}
	
	// Count rows
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	rows.Close()
	
	if rowCount != 3 {
		tx.Rollback()
		t.Fatalf("Expected 3 rows, got %d", rowCount)
	}
	
	// Test Get
	type TestRecord struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Value int    `db:"value"`
	}
	
	var record TestRecord
	err = tx.Get(&record, "SELECT id, name, value FROM tx_test WHERE id = $1", 1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to get record: %v", err)
	}
	
	if record.Name != "test1" || record.Value != 100 {
		tx.Rollback()
		t.Fatalf("Unexpected record values: %+v", record)
	}
	
	// Test Select
	var records []TestRecord
	err = tx.Select(&records, "SELECT id, name, value FROM tx_test ORDER BY id")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to select records: %v", err)
	}
	
	if len(records) != 3 {
		tx.Rollback()
		t.Fatalf("Expected 3 records, got %d", len(records))
	}
	
	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestTransactionIsolationLevels tests different isolation levels
func TestTransactionIsolationLevels(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Test read committed isolation
	ctx := context.Background()
	opts := TxOptions{
		Isolation: sql.LevelReadCommitted,
	}
	
	err = WithTxOptions(ctx, opts, func(tx *Tx) error {
		_, err := tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "read-committed", 100)
		return err
	})
	
	if err != nil {
		t.Fatalf("Read committed transaction failed: %v", err)
	}
	
	// Test repeatable read isolation
	opts.Isolation = sql.LevelRepeatableRead
	
	err = WithTxOptions(ctx, opts, func(tx *Tx) error {
		_, err := tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "repeatable-read", 200)
		return err
	})
	
	if err != nil {
		t.Fatalf("Repeatable read transaction failed: %v", err)
	}
	
	// Test serializable isolation
	opts.Isolation = sql.LevelSerializable
	
	err = WithTxOptions(ctx, opts, func(tx *Tx) error {
		_, err := tx.Exec("INSERT INTO tx_test (name, value) VALUES ($1, $2)", "serializable", 300)
		return err
	})
	
	if err != nil {
		t.Fatalf("Serializable transaction failed: %v", err)
	}
	
	// Verify all records were inserted
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	
	if count != 3 {
		t.Errorf("Expected 3 records, got %d", count)
	}
}

// TestORMWithTransaction tests ORM operations within a transaction
func TestORMWithTransaction(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_test_orm")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_test_orm (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_test_orm")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_test_orm")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Define test struct
	type TestORM struct {
		ID        int       `db:"id" dbMode:"p"`
		Name      string    `db:"name" dbMode:"iu"`
		Value     int       `db:"value" dbMode:"iu"`
		CreatedAt time.Time `db:"created_at" dbMode:"i"`
	}
	
	// Initialize model tag cache
	_, err = InitModelTagCacheForType(reflect.TypeOf(TestORM{}))
	if err != nil {
		t.Fatalf("Failed to initialize model tag cache: %v", err)
	}
	
	// Initialize the table in the model cache system
	InitModelTagCache(TestORM{}, "tx_test_orm")
	
	// Test transaction with ORM operations
	ctx := context.Background()
	
	err = WithTx(ctx, func(tx *Tx) error {
		// Insert a record
		test1 := TestORM{
			Name:      "orm-test1",
			Value:     100,
			CreatedAt: time.Now(),
		}
		
		err := InsertWithTx(tx, test1, "tx_test_orm")
		if err != nil {
			return err
		}
		
		// Update the record
		var record TestORM
		err = tx.Get(&record, "SELECT * FROM tx_test_orm WHERE name = $1", "orm-test1")
		if err != nil {
			return err
		}
		
		record.Value = 150
		err = UpdateWithTx(tx, record, "tx_test_orm", "id = $1", record.ID)
		if err != nil {
			return err
		}
		
		// Direct query instead of using builder
		var records []TestORM
		err = tx.Select(&records, "SELECT * FROM tx_test_orm WHERE value > $1 ORDER BY id", 100)
		if err != nil {
			return err
		}
		
		if len(records) != 1 {
			return errors.New("expected 1 record from query")
		}
		
		return nil
	})
	
	if err != nil {
		t.Fatalf("ORM transaction failed: %v", err)
	}
	
	// Verify the record was updated
	var record TestORM
	err = Db.Get(&record, "SELECT * FROM tx_test_orm WHERE name = $1", "orm-test1")
	if err != nil {
		t.Fatalf("Failed to get record: %v", err)
	}
	
	if record.Value != 150 {
		t.Errorf("Expected value 150, got %d", record.Value)
	}
}

// TestBatchOperationsWithTx tests batch operations within a transaction
func TestBatchOperationsWithTx(t *testing.T) {
	// Skip test if database is not configured
	if Db == nil {
		t.Skip("Database not initialized. Set environment variables to run this test.")
	}
	
	// Create test table, drop first if it exists
	_, _ = Db.Exec("DROP TABLE IF EXISTS tx_batch_test")
	_, err := Db.Exec(`
		CREATE TABLE IF NOT EXISTS tx_batch_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	
	// Clean up the table
	defer Db.Exec("DROP TABLE tx_batch_test")
	
	// Clean the table before test
	_, err = Db.Exec("TRUNCATE tx_batch_test")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
	}
	
	// Test batch insert within transaction
	ctx := context.Background()
	
	err = WithTx(ctx, func(tx *Tx) error {
		// Create batch executor
		batch := NewBatchInsert("tx_batch_test", []string{"name", "value"}, 10)
		
		// Add records
		for i := 1; i <= 20; i++ {
			err := batch.Add(map[string]interface{}{
				"name":  fmt.Sprintf("batch-test-%d", i),
				"value": i * 10,
			})
			if err != nil {
				return fmt.Errorf("failed to add object to batch: %w", err)
			}
		}
		
		// Flush remaining records within the transaction
		return batch.FlushWithTx(tx)
	})
	
	if err != nil {
		t.Fatalf("Batch transaction failed: %v", err)
	}
	
	// Verify records were inserted
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM tx_batch_test")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	
	if count != 20 {
		t.Errorf("Expected 20 records, got %d", count)
	}
	
	// Test batch update within transaction
	err = WithTx(ctx, func(tx *Tx) error {
		// Create batch executor
		batch := NewBatchUpdate("tx_batch_test", []string{"value"}, "id", 10)
		
		// Add updates
		for i := 1; i <= 20; i++ {
			err := batch.Add(map[string]interface{}{
				"value": i * 20,
			}, i)
			if err != nil {
				return fmt.Errorf("failed to add update to batch: %w", err)
			}
		}
		
		// Flush remaining records within the transaction
		return batch.FlushWithTx(tx)
	})
	
	if err != nil {
		t.Fatalf("Batch update transaction failed: %v", err)
	}
	
	// Verify records were updated
	var sum int
	err = Db.Get(&sum, "SELECT SUM(value) FROM tx_batch_test")
	if err != nil {
		t.Fatalf("Failed to sum values: %v", err)
	}
	
	expected := 0
	for i := 1; i <= 20; i++ {
		expected += i * 20
	}
	
	if sum != expected {
		t.Errorf("Expected sum %d, got %d", expected, sum)
	}
}