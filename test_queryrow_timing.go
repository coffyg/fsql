package fsql

import (
	"context"
	"testing"
	"time"
)

// Test to determine WHEN QueryRow actually executes the query
func TestQueryRowExecutionTiming(t *testing.T) {
	InitDB("postgres://postgres:postgres@localhost:5432/fsql_test?sslmode=disable")
	defer CloseDB()

	// Create context that expires immediately
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	
	// Wait for context to definitely be cancelled
	time.Sleep(10 * time.Millisecond)
	
	// Try QueryRowContext with already-cancelled context
	row := Db.QueryRowContext(ctx, "SELECT 1 as num")
	
	// Now try to scan
	var num int
	err := row.Scan(&num)
	
	if err == nil {
		t.Logf("SUCCESS: Scan worked even though context was cancelled before QueryRowContext returned")
		t.Logf("This means: Query executed DURING QueryRowContext call, result cached in row")
	} else if err == context.DeadlineExceeded || err == context.Canceled {
		t.Logf("FAILURE: Got context error during Scan: %v", err)
		t.Logf("This means: Query executes DURING Scan, not during QueryRowContext")
	} else {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// Test with valid context to see normal behavior
func TestQueryRowNormalExecution(t *testing.T) {
	InitDB("postgres://postgres:postgres@localhost:5432/fsql_test?sslmode=disable")
	defer CloseDB()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	row := Db.QueryRowContext(ctx, "SELECT 1 as num")
	
	var num int
	err := row.Scan(&num)
	
	if err != nil {
		t.Fatalf("Normal query failed: %v", err)
	}
	
	if num != 1 {
		t.Fatalf("Expected 1, got %d", num)
	}
	
	t.Logf("Normal execution works fine")
}
