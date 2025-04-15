package fsql

import (
	"fmt"
	"math"
	"testing"

	"github.com/coffyg/octypes"
)

func TestBuildFilterCountCustom(t *testing.T) {
	// Clean the database before the test
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Create test data by using the existing ai_model table
	for i := 1; i <= 20; i++ {
		model := AIModelTest{
			Key:      *octypes.NewNullString(fmt.Sprintf("key_%d", i)),
			Name:     *octypes.NewNullString(fmt.Sprintf("Model %d", i)),
			Type:     *octypes.NewNullString("test_type"),
			Provider: *octypes.NewNullString("test_provider"),
		}
		err := model.Insert()
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
	}

	// Test various query patterns similar to the one used in ListCreditHistoryForUser
	testQueryPatterns(t)
}

func testQueryPatterns(t *testing.T) {
	// Test standard query 
	baseQuery := aiModelBaseQuery + " WHERE type = $1"
	baseArgs := []interface{}{"test_type"}
	testCountQuery(t, "standard", baseQuery, baseArgs, 20)

	// Test OR condition query
	baseQuery = aiModelBaseQuery + " WHERE type = $1 OR provider = $2"
	baseArgs = []interface{}{"test_type", "test_provider"}
	testCountQuery(t, "or_condition", baseQuery, baseArgs, 20)

	// Test complex condition query with quoted table name
	baseQuery = aiModelBaseQuery + ` WHERE "ai_model".type = $1`
	baseArgs = []interface{}{"test_type"}
	testCountQuery(t, "quoted_table", baseQuery, baseArgs, 20)

	// Test query with aliased table 
	baseQuery = aiModelBaseQuery + ` WHERE "ai_model".type = $1`
	baseArgs = []interface{}{"test_type"}
	testCountQuery(t, "aliased_table", baseQuery, baseArgs, 20)
	
	// Test subquery in condition
	baseQuery = aiModelBaseQuery + ` WHERE type IN (SELECT type FROM ai_model WHERE provider = $1)`
	baseArgs = []interface{}{"test_provider"}
	testCountQuery(t, "subquery", baseQuery, baseArgs, 20)
}

func testCountQuery(t *testing.T, testName string, baseQuery string, baseArgs []interface{}, expectedCount int) {
	t.Helper()
	
	perPage := 10
	page := 1

	// Apply FilterQueryCustom to create paginated query
	query, args, err := FilterQueryCustom(
		baseQuery,
		"ai_model",
		`"ai_model".key DESC`,
		baseArgs,
		perPage,
		page,
	)
	if err != nil {
		t.Fatalf("FilterQueryCustom error for %s: %v", testName, err)
	}

	// Get some results to verify query works
	models := []AIModelTest{}
	err = Db.Select(&models, query, args...)
	if err != nil {
		t.Fatalf("Select error for %s: %v", testName, err)
	}

	// Test BuildFilterCountCustom - this is what we're fixing
	countQuery := BuildFilterCountCustom(query)
	
	// For debugging, log the generated count query
	t.Logf("Test: %s\nCount query: %s", testName, countQuery)
	
	count, err := GetFilterCount(countQuery, args)
	if err != nil {
		t.Fatalf("GetFilterCount error for %s: %v", testName, err)
	}

	// Verify results
	if count != expectedCount {
		t.Errorf("Test %s: Expected count %d, got %d", testName, expectedCount, count)
	}

	// Verify pagination correct
	pagination := octypes.Pagination{
		ResultsPerPage: perPage,
		PageNo:         page,
		Count:          count,
		PageMax:        int(math.Ceil(float64(count) / float64(perPage))),
	}

	expectedPageMax := int(math.Ceil(float64(expectedCount)/float64(perPage)))
	if pagination.PageMax != expectedPageMax {
		t.Errorf("Test %s: Expected page max %d, got %d", testName, expectedPageMax, pagination.PageMax)
	}
}