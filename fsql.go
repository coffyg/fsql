// fsql.go
package fsql

import (
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/jmoiron/sqlx" // SQL library
	_ "github.com/lib/pq"     // PostgreSQL driver
)

var Db *sqlx.DB

func PgxCreateDB(uri string) (*sqlx.DB, error) {
	connConfig, _ := pgx.ParseConfig(uri)

	pgxdb := stdlib.OpenDB(*connConfig)
	return sqlx.NewDb(pgxdb, "pgx"), nil
}

func InitDB(database string) {
	var err error
	Db, err = PgxCreateDB(database)
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
