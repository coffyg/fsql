package fsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/coffyg/octypes"
	"github.com/coffyg/utils"
)

// Benchmark for model initialization
func BenchmarkInitModelTagCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Reset the cache for each iteration
		modelFieldsCache = utils.NewOptimizedSafeMap[*modelInfo]()
		b.StopTimer()
		model := AIModelTest{}
		b.StartTimer()
		InitModelTagCache(model, "ai_model")
	}
}

// Benchmark for field generation
func BenchmarkGetSelectFields(b *testing.B) {
	// Ensure model is initialized
	InitModelTagCache(AIModelTest{}, "ai_model")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetSelectFields("ai_model", "")
	}
}

// Benchmark for field generation with alias
func BenchmarkGetSelectFieldsWithAlias(b *testing.B) {
	// Ensure model is initialized
	InitModelTagCache(AIModelTest{}, "ai_model")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetSelectFields("ai_model", "a")
	}
}

// Benchmark for query building
func BenchmarkSelectBaseQueryBuild(b *testing.B) {
	// Ensure models are initialized
	InitModelTagCache(WebsiteTest{}, "website")
	InitModelTagCache(RealmTest{}, "realm")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SelectBase("website", "").
			Left("realm", "r", "website.realm_uuid = r.uuid").
			Where("website.domain LIKE '%com'").
			Build()
	}
}

// Benchmark for filtering
func BenchmarkFilterQuery(b *testing.B) {
	// Ensure model is initialized
	InitModelTagCache(AIModelTest{}, "ai_model")
	
	baseQuery := SelectBase("ai_model", "").Build()
	filters := &Filter{
		"Type":              "test_type",
		"Provider[$prefix]": "test",
		"Key[$in]":          []string{"key1", "key2", "key3"},
	}
	sort := &Sort{
		"Key": "ASC",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FilterQuery(baseQuery, "ai_model", filters, sort, "ai_model", 10, 1)
	}
}

// Benchmark for insert query generation
func BenchmarkGetInsertQuery(b *testing.B) {
	// Ensure model is initialized
	InitModelTagCache(AIModelTest{}, "ai_model")
	
	// Prepare values
	valuesMap := map[string]interface{}{
		"uuid":        GenNewUUID(""),
		"key":         octypes.NewNullString("test_key"),
		"name":        octypes.NewNullString("Test Model"),
		"description": octypes.NewNullString("Test Description"),
		"type":        octypes.NewNullString("test_type"),
		"provider":    octypes.NewNullString("test_provider"),
		"settings":    octypes.NewNullString("{}"),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetInsertQuery("ai_model", valuesMap, "uuid")
	}
}

// Benchmark for update query generation
func BenchmarkGetUpdateQuery(b *testing.B) {
	// Ensure model is initialized
	InitModelTagCache(AIModelTest{}, "ai_model")
	
	// Prepare values
	uuid := GenNewUUID("")
	valuesMap := map[string]interface{}{
		"uuid":        uuid,
		"key":         octypes.NewNullString("updated_key"),
		"name":        octypes.NewNullString("Updated Model"),
		"description": octypes.NewNullString("Updated Description"),
		"type":        octypes.NewNullString("updated_type"),
		"provider":    octypes.NewNullString("updated_provider"),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetUpdateQuery("ai_model", valuesMap, "uuid")
	}
}

// Benchmark for end-to-end listing with filters
func BenchmarkListWithFilters(b *testing.B) {
	// Skip if not connected to DB
	if Db == nil {
		b.Skip("Database not connected, skipping test")
	}
	
	// Clean the database before the benchmark
	if err := cleanDatabase(); err != nil {
		b.Fatalf("Failed to clean database: %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		aiModel := AIModelTest{
			Key:      *octypes.NewNullString(fmt.Sprintf("key_%d", i)),
			Name:     *octypes.NewNullString(fmt.Sprintf("Model %d", i)),
			Type:     *octypes.NewNullString("test_type"),
			Provider: *octypes.NewNullString("test_provider"),
		}
		if err := aiModel.Insert(); err != nil {
			b.Fatalf("Insert error: %v", err)
		}
	}
	
	// Prepare filter parameters
	filters := &Filter{
		"Type": "test_type",
	}
	sort := &Sort{
		"Key": "ASC",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		models, _, err := ListAIModel(filters, sort, 10, 2)
		if err != nil {
			b.Fatalf("ListAIModel error: %v", err)
		}
		if len(*models) != 10 {
			b.Fatalf("Expected 10 models, got %d", len(*models))
		}
	}
}

// Benchmark for complex join query
func BenchmarkComplexJoinQuery(b *testing.B) {
	// Skip if not connected to DB
	if Db == nil {
		b.Skip("Database not connected, skipping test")
	}
	
	// Clean the database before the benchmark
	if err := cleanDatabase(); err != nil {
		b.Fatalf("Failed to clean database: %v", err)
	}

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
}

// Helper functions for benchmarks
func insertRealmBench(b *testing.B, realm RealmTest) {
	query, args := GetInsertQuery("realm", map[string]interface{}{
		"uuid":       realm.UUID,
		"name":       realm.Name,
		"created_at": realm.CreatedAt,
		"updated_at": realm.UpdatedAt,
	}, "")
	_, err := Db.Exec(query, args...)
	if err != nil {
		b.Fatalf("Failed to insert realm: %v", err)
	}
}

func insertWebsiteBench(b *testing.B, website WebsiteTest) {
	query, args := GetInsertQuery("website", map[string]interface{}{
		"uuid":       website.UUID,
		"domain":     website.Domain,
		"realm_uuid": website.RealmUUID,
		"created_at": website.CreatedAt,
		"updated_at": website.UpdatedAt,
	}, "")
	_, err := Db.Exec(query, args...)
	if err != nil {
		b.Fatalf("Failed to insert website: %v", err)
	}
}