package fsql

import (
	"context"
	"testing"
	"time"

	"github.com/coffyg/octypes"
)

func TestQueryRowCancelImmediately(t *testing.T) {
	// Clean database first
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Insert test model
	testModel := AIModelTest{
		Key:      *octypes.NewNullString("test-cancel-key"),
		Name:     *octypes.NewNullString("Test Cancel Model"),
		Type:     *octypes.NewNullString("image"),
		Provider: *octypes.NewNullString("test"),
	}
	err := testModel.Insert()
	if err != nil {
		t.Fatalf("Failed to insert test model: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	row := Db.QueryRowContext(ctx, "SELECT key, name, type FROM ai_model WHERE key = $1", testModel.Key.String)
	cancel()  // Cancel RIGHT after QueryRowContext returns

	var key, name, modelType string
	err = row.Scan(&key, &name, &modelType)

	if err != nil {
		t.Fatalf("PROVEN: Scan failed after immediate cancel: %v\nThis means query executes DURING Scan, NOT during QueryRowContext", err)
	}

	if key != testModel.Key.String || name != testModel.Name.String || modelType != testModel.Type.String {
		t.Fatalf("Expected (%s, %s, %s), got (%s, %s, %s)", testModel.Key.String, testModel.Name.String, testModel.Type.String, key, name, modelType)
	}

	t.Logf("PROVEN: Scan worked after immediate cancel - query executed DURING QueryRowContext, result already in memory")
}
