// fsql.go
package fsql

import (
	"log"

	"github.com/jmoiron/sqlx" // SQL library
	_ "github.com/lib/pq"     // PostgreSQL driver
)

var Db *sqlx.DB

func InitDB(database string) {
	var err error
	Db, err = sqlx.Connect("postgres", database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Set reasonable limits
	/*
		Db.SetMaxOpenConns(100)
		Db.SetMaxIdleConns(50)
		Db.SetConnMaxLifetime(60 * time.Minute)
	*/
}

// CloseDB closes the database connection
func CloseDB() {
	if Db != nil {
		if err := Db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}
}
