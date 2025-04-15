package fsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/coffyg/octypes"
)

// Benchmark to compare standard insert vs batch insert
func BenchmarkBatchInsert(b *testing.B) {
	// Skip if not connected to DB
	if Db == nil {
		b.Skip("Database not connected, skipping test")
	}
	
	// Clean the database before the benchmark
	if err := cleanDatabase(); err != nil {
		b.Fatalf("Failed to clean database: %v", err)
	}
	
	// Define fields for realm table
	fields := []string{"uuid", "name", "created_at", "updated_at"}
	
	// Number of records to insert
	recordCount := 100
	
	// Create test data
	testData := make([]map[string]interface{}, recordCount)
	for i := 0; i < recordCount; i++ {
		testData[i] = map[string]interface{}{
			"uuid":       GenNewUUID(""),
			"name":       fmt.Sprintf("Realm %d", i),
			"created_at": octypes.NewCustomTime(time.Now()),
			"updated_at": octypes.NewCustomTime(time.Now()),
		}
	}
	
	// Benchmark standard insert (one by one)
	b.Run("StandardInsert", func(b *testing.B) {
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Reset database for each iteration
			if err := cleanDatabase(); err != nil {
				b.Fatalf("Failed to clean database: %v", err)
			}
			
			// Insert records one by one
			for _, data := range testData {
				query, args := GetInsertQuery("realm", data, "")
				_, err := Db.Exec(query, args...)
				if err != nil {
					b.Fatalf("Failed to insert: %v", err)
				}
			}
		}
	})
	
	// Benchmark batch insert (using the batch executor)
	b.Run("BatchInsert", func(b *testing.B) {
		// Batch sizes to test
		batchSizes := []int{10, 50, 100}
		
		for _, batchSize := range batchSizes {
			b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
				b.ResetTimer()
				
				for i := 0; i < b.N; i++ {
					// Reset database for each iteration
					if err := cleanDatabase(); err != nil {
						b.Fatalf("Failed to clean database: %v", err)
					}
					
					// Create batch executor
					batchInsert := NewBatchInsert("realm", fields, batchSize)
					
					// Add records to batch
					for _, data := range testData {
						if err := batchInsert.Add(data); err != nil {
							b.Fatalf("Failed to add to batch: %v", err)
						}
					}
					
					// Flush any remaining records
					if err := batchInsert.Flush(); err != nil {
						b.Fatalf("Failed to flush batch: %v", err)
					}
				}
			})
		}
	})
	
	// Benchmark using pooled batch executors
	b.Run("PooledBatchInsert", func(b *testing.B) {
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Reset database for each iteration
			if err := cleanDatabase(); err != nil {
				b.Fatalf("Failed to clean database: %v", err)
			}
			
			// Get batch executor from pool
			batchInsert := GetBatchInsert("realm", fields, 50)
			
			// Add records to batch
			for _, data := range testData {
				if err := batchInsert.Add(data); err != nil {
					b.Fatalf("Failed to add to batch: %v", err)
				}
			}
			
			// Flush any remaining records
			if err := batchInsert.Flush(); err != nil {
				b.Fatalf("Failed to flush batch: %v", err)
			}
			
			// Return to pool
			ReleaseBatchInsert(batchInsert)
		}
	})
}

// Benchmark to compare standard update vs batch update
func BenchmarkBatchUpdate(b *testing.B) {
	// Skip if not connected to DB
	if Db == nil {
		b.Skip("Database not connected, skipping test")
	}
	
	// Clean the database before the benchmark
	if err := cleanDatabase(); err != nil {
		b.Fatalf("Failed to clean database: %v", err)
	}
	
	// Number of records to insert
	recordCount := 100
	
	// Create test data and insert records
	testData := make([]RealmTest, recordCount)
	for i := 0; i < recordCount; i++ {
		testData[i] = RealmTest{
			UUID:      GenNewUUID(""),
			Name:      fmt.Sprintf("Original Realm %d", i),
			CreatedAt: octypes.NewCustomTime(time.Now()),
			UpdatedAt: octypes.NewCustomTime(time.Now()),
		}
		// Insert the realm manually
		query, args := GetInsertQuery("realm", map[string]interface{}{
			"uuid":       testData[i].UUID,
			"name":       testData[i].Name,
			"created_at": testData[i].CreatedAt,
			"updated_at": testData[i].UpdatedAt,
		}, "")
		_, err := Db.Exec(query, args...)
		if err != nil {
			b.Fatalf("Failed to insert realm: %v", err)
		}
	}
	
	// Create update data
	updateFields := []string{"name", "updated_at"}
	updateData := make([]map[string]interface{}, recordCount)
	for i := 0; i < recordCount; i++ {
		updateData[i] = map[string]interface{}{
			"name":       fmt.Sprintf("Updated Realm %d", i),
			"updated_at": octypes.NewCustomTime(time.Now()),
		}
	}
	
	// Benchmark standard update (one by one)
	b.Run("StandardUpdate", func(b *testing.B) {
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Update records one by one
			for j, data := range updateData {
				// Create combined map with all fields including the condition field
				valueMap := map[string]interface{}{
					"name":       data["name"],
					"updated_at": data["updated_at"],
					"uuid":       testData[j].UUID,
				}
				
				query, args := GetUpdateQuery("realm", valueMap, "uuid")
				
				_, err := Db.Exec(query, args...)
				if err != nil {
					b.Fatalf("Failed to update: %v", err)
				}
			}
		}
	})
	
	// Benchmark batch update (using the batch executor)
	b.Run("BatchUpdate", func(b *testing.B) {
		// Batch sizes to test
		batchSizes := []int{10, 50, 100}
		
		for _, batchSize := range batchSizes {
			b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
				b.ResetTimer()
				
				for i := 0; i < b.N; i++ {
					// Create batch executor
					batchUpdate := NewBatchUpdate("realm", updateFields, "uuid", batchSize)
					
					// Add records to batch
					for j, data := range updateData {
						if err := batchUpdate.Add(data, testData[j].UUID); err != nil {
							b.Fatalf("Failed to add to batch: %v", err)
						}
					}
					
					// Flush any remaining records
					if err := batchUpdate.Flush(); err != nil {
						b.Fatalf("Failed to flush batch: %v", err)
					}
				}
			})
		}
	})
}