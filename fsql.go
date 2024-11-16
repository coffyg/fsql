// fsql.go
package fsql

import (
	"log"

	"math/rand"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx" // SQL library
	_ "github.com/lib/pq"     // PostgreSQL driver
)

var Db *sqlx.DB
var readReplicasDbs []*sqlx.DB

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
}
func InitDbReplicas(databases []string) {
	for _, dbURI := range databases {
		replicaDb, err := PgxCreateDB(dbURI)
		if err != nil {
			log.Printf("Failed to connect to replica database %s: %v", dbURI, err)
			continue
		}
		readReplicasDbs = append(readReplicasDbs, replicaDb)
	}
}
func GetReplika() *sqlx.DB {
	if len(readReplicasDbs) == 0 {
		return Db
	}
	idx := rand.Intn(len(readReplicasDbs))
	return readReplicasDbs[idx]
}

// CloseDB closes the database connection
func CloseDB() {
	if Db != nil {
		if err := Db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}
}
func CloseReplicas() {
	for _, db := range readReplicasDbs {
		if db != nil {
			if err := db.Close(); err != nil {
				log.Printf("Error closing replica database: %v", err)
			}
		}
	}
}
