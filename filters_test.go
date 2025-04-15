package fsql

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/coffyg/octypes"
)

type UserCreditHistory struct {
	UUID      string              `json:"UUID" db:"uuid" dbMode:"i"`
	CreatedAt *octypes.CustomTime `json:"CreatedAt" db:"created_at" dbMode:"i" dbInsertValue:"NOW()"`
	FromUUID  string              `json:"FromUUID" db:"from_uuid" dbMode:"i"`
	ToUUID    string              `json:"ToUUID" db:"to_uuid" dbMode:"i"`
	Amount    int64               `json:"Amount" db:"amount" dbMode:"i"`
	Comment   string              `json:"Comment" db:"comment" dbMode:"i"`
}

var userCreditHistoryBaseQuery string

func initUserCreditHistoryModel() {
	InitModelTagCache(UserCreditHistory{}, "user_credit_history")
	userCreditHistoryBaseQuery = SelectBase("user_credit_history", "").Build()
}

func TestFilterQueryCustomAndBuildFilterCountCustom(t *testing.T) {
	// Initialize model
	initUserCreditHistoryModel()

	// Clean the database
	if err := cleanDatabase(); err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	// Insert test data
	userUUID := GenNewUUID("")
	otherUserUUID := GenNewUUID("")
	
	// Insert multiple credit history records
	for i := 1; i <= 10; i++ {
		history := UserCreditHistory{
			UUID:     GenNewUUID(""),
			FromUUID: userUUID,
			ToUUID:   otherUserUUID,
			Amount:   int64(i * 100),
			Comment:  fmt.Sprintf("Test transaction %d", i),
			CreatedAt: octypes.NewCustomTime(time.Now()),
		}
		
		query, args := GetInsertQuery("user_credit_history", map[string]interface{}{
			"uuid":       history.UUID,
			"from_uuid":  history.FromUUID,
			"to_uuid":    history.ToUUID,
			"amount":     history.Amount,
			"comment":    history.Comment,
			"created_at": history.CreatedAt,
		}, "")
		
		_, err := Db.Exec(query, args...)
		if err != nil {
			t.Fatalf("Failed to insert credit history: %v", err)
		}
	}

	// Test FilterQueryCustom and BuildFilterCountCustom for "all" type
	testCreditHistoryQuery(t, "all", userUUID, 10)
	
	// Test FilterQueryCustom and BuildFilterCountCustom for "in" type
	testCreditHistoryQuery(t, "in", otherUserUUID, 10)
	
	// Test FilterQueryCustom and BuildFilterCountCustom for "out" type
	testCreditHistoryQuery(t, "out", userUUID, 10)
}

func testCreditHistoryQuery(t *testing.T, typeTransaction, userUUID string, expectedCount int) {
	t.Helper()
	
	var query string
	var baseArgs []interface{}

	if typeTransaction == "all" {
		query = userCreditHistoryBaseQuery + " WHERE to_uuid = $1 OR from_uuid = $1"
		baseArgs = []interface{}{userUUID}
	} else if typeTransaction == "in" {
		query = userCreditHistoryBaseQuery + " WHERE to_uuid = $1"
		baseArgs = []interface{}{userUUID}
	} else if typeTransaction == "out" {
		query = userCreditHistoryBaseQuery + " WHERE from_uuid = $1"
		baseArgs = []interface{}{userUUID}
	}

	perPage := 5
	page := 1

	// Test FilterQueryCustom
	query, args, err := FilterQueryCustom(
		query,
		"user_credit_history",
		`"user_credit_history".created_at DESC`,
		baseArgs,
		perPage,
		page,
	)
	if err != nil {
		t.Fatalf("FilterQueryCustom error for type %s: %v", typeTransaction, err)
	}

	// Fetch records
	history := []UserCreditHistory{}
	err = Db.Select(&history, query, args...)
	if err != nil {
		t.Fatalf("Select error for type %s: %v", typeTransaction, err)
	}

	// Test BuildFilterCountCustom
	countQuery := BuildFilterCountCustom(query)
	count, err := GetFilterCount(countQuery, args)
	if err != nil {
		t.Fatalf("GetFilterCount error for type %s: %v", typeTransaction, err)
	}

	// Verify results
	if count != expectedCount {
		t.Errorf("Expected count %d, got %d for type %s", expectedCount, count, typeTransaction)
	}

	pagination := octypes.Pagination{
		ResultsPerPage: perPage,
		PageNo:         page,
		Count:          count,
		PageMax:        int(math.Ceil(float64(count) / float64(perPage))),
	}

	if pagination.PageMax != int(math.Ceil(float64(expectedCount)/float64(perPage))) {
		t.Errorf("Incorrect pagination calculation for type %s", typeTransaction)
	}

	if len(history) != perPage {
		t.Errorf("Expected %d records, got %d for type %s", perPage, len(history), typeTransaction)
	}
}