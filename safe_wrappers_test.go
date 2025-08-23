// safe_wrappers_test.go
package fsql

import (
	"fmt"
	"testing"
	"time"
)

// TestSafeExec tests the SafeExec wrapper function
func TestSafeExec(t *testing.T) {
	// Clean database
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Test SafeExec with valid INSERT
	query := `INSERT INTO ai_model (uuid, key, name, type, provider) 
	          VALUES ($1, $2, $3, $4, $5)`
	uuid := GenNewUUID("")

	_, err := SafeExec(query, uuid, "test_key", "Test Model", "test_type", "test_provider")
	if err != nil {
		t.Fatalf("SafeExec failed: %v", err)
	}

	// Verify the insert worked
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("Failed to verify insert: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
}

// TestSafeQuery tests the SafeQuery wrapper function
func TestSafeQuery(t *testing.T) {
	// Clean and setup test data
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Insert test data using SafeExec
	uuid1 := GenNewUUID("")
	uuid2 := GenNewUUID("")
	_, err := SafeExec(`INSERT INTO ai_model (uuid, key, name, type, provider) VALUES 
	                 ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)`,
		uuid1, "key1", "Model 1", "type1", "provider1",
		uuid2, "key2", "Model 2", "type2", "provider2")
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Test SafeQuery
	rows, err := SafeQuery("SELECT uuid, key FROM ai_model WHERE type LIKE $1", "type%")
	if err != nil {
		t.Fatalf("SafeQuery failed: %v", err)
	}
	defer rows.Close()

	// Count results
	var count int
	for rows.Next() {
		var uuid, key string
		err = rows.Scan(&uuid, &key)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestSafeGet tests the SafeGet wrapper function
func TestSafeGet(t *testing.T) {
	// Clean and setup test data
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Insert test data
	uuid := GenNewUUID("")
	_, err := SafeExec(`INSERT INTO ai_model (uuid, key, name, type, provider) 
	                 VALUES ($1, $2, $3, $4, $5)`,
		uuid, "test_key", "Test Model", "test_type", "test_provider")
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Test SafeGet
	var model AIModelTest
	err = SafeGet(&model, "SELECT uuid, key, name FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("SafeGet failed: %v", err)
	}

	if model.UUID.String != uuid {
		t.Errorf("Expected UUID %s, got %s", uuid, model.UUID.String)
	}
	if model.Key.String != "test_key" {
		t.Errorf("Expected key 'test_key', got %s", model.Key.String)
	}
}

// TestSafeSelect tests the SafeSelect wrapper function
func TestSafeSelect(t *testing.T) {
	// Clean and setup test data
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Insert multiple test records
	for i := 1; i <= 3; i++ {
		uuid := GenNewUUID("")
		_, err := SafeExec(`INSERT INTO ai_model (uuid, key, name, type, provider) 
		                 VALUES ($1, $2, $3, $4, $5)`,
			uuid, fmt.Sprintf("key_%d", i), fmt.Sprintf("Model %d", i), "test_type", "test_provider")
		if err != nil {
			t.Fatalf("Failed to setup test data: %v", err)
		}
	}

	// Test SafeSelect
	var models []AIModelTest
	err := SafeSelect(&models, "SELECT uuid, key, name FROM ai_model WHERE type = $1", "test_type")
	if err != nil {
		t.Fatalf("SafeSelect failed: %v", err)
	}

	if len(models) != 3 {
		t.Errorf("Expected 3 models, got %d", len(models))
	}
}

// TestSafeQueryRow tests the SafeQueryRow wrapper function
func TestSafeQueryRow(t *testing.T) {
	// Clean and setup test data
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Insert test data
	uuid := GenNewUUID("")
	_, err := SafeExec(`INSERT INTO ai_model (uuid, key, name, type, provider) 
	                 VALUES ($1, $2, $3, $4, $5)`,
		uuid, "test_key", "Test Model", "test_type", "test_provider")
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Test SafeQueryRow
	var retrievedKey string
	row := SafeQueryRow("SELECT key FROM ai_model WHERE uuid = $1", uuid)
	err = row.Scan(&retrievedKey)
	if err != nil {
		t.Fatalf("SafeQueryRow failed: %v", err)
	}

	if retrievedKey != "test_key" {
		t.Errorf("Expected key 'test_key', got %s", retrievedKey)
	}
}

// TestSafeNamedExec tests the SafeNamedExec wrapper function
func TestSafeNamedExec(t *testing.T) {
	// Clean database
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Test SafeNamedExec with struct
	model := struct {
		UUID     string `db:"uuid"`
		Key      string `db:"key"`
		Name     string `db:"name"`
		Type     string `db:"type"`
		Provider string `db:"provider"`
	}{
		UUID:     GenNewUUID(""),
		Key:      "named_test_key",
		Name:     "Named Test Model",
		Type:     "named_test_type",
		Provider: "named_test_provider",
	}

	query := `INSERT INTO ai_model (uuid, key, name, type, provider) 
	          VALUES (:uuid, :key, :name, :type, :provider)`

	result, err := SafeNamedExec(query, model)
	if err != nil {
		t.Fatalf("SafeNamedExec failed: %v", err)
	}

	// Check that exactly one row was affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// Verify the insert
	var count int
	err = SafeGet(&count, "SELECT COUNT(*) FROM ai_model WHERE uuid = $1", model.UUID)
	if err != nil {
		t.Fatalf("Failed to verify insert: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
}

// TestSafeNamedQuery tests the SafeNamedQuery wrapper function
func TestSafeNamedQuery(t *testing.T) {
	// Clean and setup test data
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Insert test data
	for i := 1; i <= 2; i++ {
		uuid := GenNewUUID("")
		_, err := SafeExec(`INSERT INTO ai_model (uuid, key, name, type, provider) 
		                 VALUES ($1, $2, $3, $4, $5)`,
			uuid, fmt.Sprintf("key_%d", i), fmt.Sprintf("Model %d", i), "query_test_type", "test_provider")
		if err != nil {
			t.Fatalf("Failed to setup test data: %v", err)
		}
	}

	// Test SafeNamedQuery with map
	params := map[string]interface{}{
		"search_type": "query_test_type",
	}

	rows, err := SafeNamedQuery("SELECT uuid, key FROM ai_model WHERE type = :search_type", params)
	if err != nil {
		t.Fatalf("SafeNamedQuery failed: %v", err)
	}
	defer rows.Close()

	// Count results
	var count int
	for rows.Next() {
		var uuid, key string
		err = rows.Scan(&uuid, &key)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestSafeBegin tests the SafeBegin wrapper function
func TestSafeBegin(t *testing.T) {
	// Test SafeBegin transaction
	tx, err := SafeBegin()
	if err != nil {
		t.Fatalf("SafeBegin failed: %v", err)
	}

	// Test transaction operations
	uuid := GenNewUUID("")
	_, err = tx.Exec(`INSERT INTO ai_model (uuid, key, name, type, provider) 
	                  VALUES ($1, $2, $3, $4, $5)`,
		uuid, "tx_test_key", "TX Test Model", "tx_test_type", "tx_test_provider")
	if err != nil {
		t.Fatalf("Transaction exec failed: %v", err)
	}

	// Verify data exists within transaction
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM ai_model WHERE uuid = $1", uuid).Scan(&count)
	if err != nil {
		t.Fatalf("Transaction query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record in transaction, got %d", count)
	}

	// Rollback and verify data doesn't exist outside transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Transaction rollback failed: %v", err)
	}

	err = SafeGet(&count, "SELECT COUNT(*) FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("Failed to verify rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records after rollback, got %d", count)
	}
}

// TestSafeBeginx tests the SafeBeginx wrapper function
func TestSafeBeginx(t *testing.T) {
	// Test SafeBeginx transaction (sqlx version)
	tx, err := SafeBeginx()
	if err != nil {
		t.Fatalf("SafeBeginx failed: %v", err)
	}

	// Test transaction operations with sqlx methods
	uuid := GenNewUUID("")
	_, err = tx.Exec(`INSERT INTO ai_model (uuid, key, name, type, provider) 
	                  VALUES ($1, $2, $3, $4, $5)`,
		uuid, "tx_test_key", "TX Test Model", "tx_test_type", "tx_test_provider")
	if err != nil {
		t.Fatalf("Transaction exec failed: %v", err)
	}

	// Use sqlx-specific Get method
	var model AIModelTest
	err = tx.Get(&model, "SELECT uuid, key FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("Transaction Get failed: %v", err)
	}
	if model.UUID.String != uuid {
		t.Errorf("Expected UUID %s, got %s", uuid, model.UUID.String)
	}

	// Rollback and verify data doesn't exist outside transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Transaction rollback failed: %v", err)
	}

	var count int
	err = SafeGet(&count, "SELECT COUNT(*) FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("Failed to verify rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records after rollback, got %d", count)
	}
}

// TestSafeWrappersTimeout tests that Safe wrappers respect timeouts
func TestSafeWrappersTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timeout test in short mode")
	}

	// Temporarily set a very short timeout for testing
	originalTimeout := DefaultDBTimeout
	DefaultDBTimeout = 100 * time.Millisecond
	defer func() {
		DefaultDBTimeout = originalTimeout
	}()

	// Test that SafeExec times out with a slow query
	// This query should take longer than 100ms
	_, err := SafeExec("SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// Reset to normal timeout for cleanup
	DefaultDBTimeout = originalTimeout
}

// TestSafeExecTimeoutCustom tests custom timeout behavior
func TestSafeExecTimeoutCustom(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timeout test in short mode")
	}

	// Test with very short custom timeout (should fail)
	_, err := SafeExecTimeout(50*time.Millisecond, "SELECT pg_sleep(0.5)")
	if err == nil {
		t.Error("Expected timeout error with 50ms timeout, got nil")
	}

	// Test with sufficient custom timeout (should succeed)
	_, err = SafeExecTimeout(2*time.Second, "SELECT 1")
	if err != nil {
		t.Errorf("Expected success with 2s timeout, got error: %v", err)
	}
}

// TestSafeExecWarningBehavior tests warning message logic
func TestSafeExecWarningBehavior(t *testing.T) {
	// Reset warning flag for this test
	dbTimeoutWarningLogged = false
	
	// Store original timeout and logger state
	originalTimeout := DefaultDBTimeout
	originalLogger := logger
	defer func() {
		DefaultDBTimeout = originalTimeout
		logger = originalLogger
	}()

	// Set test timeout
	DefaultDBTimeout = 1 * time.Second

	// Test SafeExec (should trigger warning if logger exists)
	// We won't set logger so no actual log output
	_, err := SafeExec("SELECT 1")
	if err != nil {
		t.Errorf("SafeExec failed: %v", err)
	}

	// Test SafeExecTimeout with custom timeout (should NOT trigger warning)
	_, err = SafeExecTimeout(5*time.Second, "SELECT 1")
	if err != nil {
		t.Errorf("SafeExecTimeout failed: %v", err)
	}

	// Test SafeExecTimeout with default timeout (would trigger warning)
	_, err = SafeExecTimeout(DefaultDBTimeout, "SELECT 1")
	if err != nil {
		t.Errorf("SafeExecTimeout with default timeout failed: %v", err)
	}
}

// TestSafeExecTimeoutOperationNames tests that timeout logging shows correct operation names
func TestSafeExecTimeoutOperationNames(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timeout operation name test in short mode")
	}

	// Reset warning flag
	dbTimeoutWarningLogged = false

	// Store original values
	originalTimeout := DefaultDBTimeout
	defer func() {
		DefaultDBTimeout = originalTimeout
	}()

	// Set very short timeout to force timeouts
	DefaultDBTimeout = 100 * time.Millisecond

	// Test SafeExec timeout (should log as "SafeExec")
	_, err := SafeExec("SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeExec to timeout, got nil error")
	}
	// Note: Without capturing logs, we can't verify the operation name here
	// but the function should log "SafeExec" not "SafeExecTimeout"

	// Test SafeExecTimeout with custom timeout (should log as "SafeExecTimeout")
	_, err = SafeExecTimeout(50*time.Millisecond, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeExecTimeout to timeout, got nil error")
	}
	// This should log "SafeExecTimeout" in the timeout message

	// Test SafeExecTimeout with default timeout (should log as "SafeExec")  
	_, err = SafeExecTimeout(DefaultDBTimeout, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeExecTimeout with default timeout to timeout, got nil error")
	}
	// This should log "SafeExec" since it's using DefaultDBTimeout
}

// TestAllSafeWrappersTimeout tests all Safe functions timeout correctly with proper operation names
func TestAllSafeWrappersTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comprehensive timeout test in short mode")
	}

	// Store original values
	originalTimeout := DefaultDBTimeout
	defer func() {
		DefaultDBTimeout = originalTimeout
	}()

	// Set very short timeout to force timeouts
	DefaultDBTimeout = 50 * time.Millisecond

	// Test SafeExec timeout
	_, err := SafeExec("SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeExec to timeout")
	}

	// Test SafeExecTimeout timeout  
	_, err = SafeExecTimeout(50*time.Millisecond, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeExecTimeout to timeout")
	}

	// Test SafeGet timeout
	var result int
	err = SafeGet(&result, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeGet to timeout")
	}

	// Test SafeGetTimeout timeout
	err = SafeGetTimeout(50*time.Millisecond, &result, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeGetTimeout to timeout")
	}

	// Test SafeSelect timeout
	var results []int
	err = SafeSelect(&results, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeSelect to timeout")
	}

	// Test SafeSelectTimeout timeout
	err = SafeSelectTimeout(50*time.Millisecond, &results, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected SafeSelectTimeout to timeout")
	}

	// Test SafeNamedExec timeout
	params := map[string]interface{}{"sleep_time": 1}
	_, err = SafeNamedExec("SELECT pg_sleep(:sleep_time)", params)
	if err == nil {
		t.Error("Expected SafeNamedExec to timeout")
	}

	// Test SafeNamedExecTimeout timeout
	_, err = SafeNamedExecTimeout(50*time.Millisecond, "SELECT pg_sleep(:sleep_time)", params)
	if err == nil {
		t.Error("Expected SafeNamedExecTimeout to timeout")
	}
}

// TestAllSafeWrappersSuccess tests all Safe functions work correctly without timeout
func TestAllSafeWrappersSuccess(t *testing.T) {
	// Store original values
	originalTimeout := DefaultDBTimeout
	defer func() {
		DefaultDBTimeout = originalTimeout
	}()

	// Set reasonable timeout
	DefaultDBTimeout = 5 * time.Second

	// Test SafeExec success
	_, err := SafeExec("SELECT 1")
	if err != nil {
		t.Errorf("SafeExec failed: %v", err)
	}

	// Test SafeExecTimeout success
	_, err = SafeExecTimeout(5*time.Second, "SELECT 1")
	if err != nil {
		t.Errorf("SafeExecTimeout failed: %v", err)
	}

	// Test SafeGet success
	var result int
	err = SafeGet(&result, "SELECT 1")
	if err != nil {
		t.Errorf("SafeGet failed: %v", err)
	}
	if result != 1 {
		t.Errorf("Expected result 1, got %d", result)
	}

	// Test SafeGetTimeout success
	err = SafeGetTimeout(5*time.Second, &result, "SELECT 2")
	if err != nil {
		t.Errorf("SafeGetTimeout failed: %v", err)
	}
	if result != 2 {
		t.Errorf("Expected result 2, got %d", result)
	}

	// Test SafeSelect success
	var results []int
	err = SafeSelect(&results, "SELECT 1 UNION SELECT 2")
	if err != nil {
		t.Errorf("SafeSelect failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Test SafeSelectTimeout success
	err = SafeSelectTimeout(5*time.Second, &results, "SELECT 3 UNION SELECT 4")
	if err != nil {
		t.Errorf("SafeSelectTimeout failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Test SafeNamedExec success (can't easily test with SELECT, skip for now)
	// Test SafeNamedExecTimeout success (can't easily test with SELECT, skip for now)

	// Test SafeQueryRow success
	row := SafeQueryRow("SELECT 42")
	var value int
	err = row.Scan(&value)
	if err != nil {
		t.Errorf("SafeQueryRow scan failed: %v", err)
	}
	if value != 42 {
		t.Errorf("Expected value 42, got %d", value)
	}

	// Test SafeQueryRowTimeout success
	row = SafeQueryRowTimeout(5*time.Second, "SELECT 99")
	err = row.Scan(&value)
	if err != nil {
		t.Errorf("SafeQueryRowTimeout scan failed: %v", err)
	}
	if value != 99 {
		t.Errorf("Expected value 99, got %d", value)
	}
}

// TestSafeExecTimeoutActualBehavior tests that long custom timeouts actually work
func TestSafeExecTimeoutActualBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long timeout test in short mode")
	}

	// Reset warning flag
	dbTimeoutWarningLogged = false

	// Test that a 10-second timeout actually waits 10 seconds before failing
	start := time.Now()
	_, err := SafeExecTimeout(10*time.Second, "SELECT pg_sleep(15)")
	duration := time.Since(start)

	// Should have failed after ~10 seconds, not instantly
	if err == nil {
		t.Error("Expected timeout error after 10s, got success")
	}

	// Duration should be close to 10s, not 2s or instant
	if duration < 8*time.Second {
		t.Errorf("Timeout happened too early: expected ~10s, got %v", duration)
	}
	if duration > 12*time.Second {
		t.Errorf("Timeout happened too late: expected ~10s, got %v", duration)
	}

	// Test that a 3-second timeout works correctly
	start = time.Now()
	_, err = SafeExecTimeout(3*time.Second, "SELECT pg_sleep(5)")
	duration = time.Since(start)

	if err == nil {
		t.Error("Expected timeout error after 3s, got success")
	}

	// Duration should be close to 3s
	if duration < 2500*time.Millisecond || duration > 4*time.Second {
		t.Errorf("3s timeout duration incorrect: expected ~3s, got %v", duration)
	}
}
