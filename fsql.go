// fsql.go
package fsql

import (
	"context"
	"log"

	"math/rand"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx" // SQL library
)

var Db *sqlx.DB
var readReplicasDbs []*sqlx.DB

func PgxCreateDBWithPool(uri string) (*sqlx.DB, error) {
	// Create a connection pool
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(context.Background(), config.ConnString())
	if err != nil {
		return nil, err
	}

	// Wrap pgxpool.Pool as sqlx.DB using stdlib
	return sqlx.NewDb(stdlib.OpenDBFromPool(pool), "pgx"), nil
}

func PgxCreateDB(uri string) (*sqlx.DB, error) {
	connConfig, _ := pgx.ParseConfig(uri)

	pgxdb := stdlib.OpenDB(*connConfig)
	return sqlx.NewDb(pgxdb, "pgx"), nil
}

func InitDBPool(database string) {
	var err error
	Db, err = PgxCreateDBWithPool(database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}

func InitCustomDb(database string) *sqlx.DB {
	db, err := PgxCreateDB(database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	return db
}

func InitDB(database string) {
	var err error
	Db, err = PgxCreateDB(database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}
func CloseDB() {
	if Db != nil {
		if err := Db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
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

func CloseReplicas() {
	for _, db := range readReplicasDbs {
		if db != nil {
			if err := db.Close(); err != nil {
				log.Printf("Error closing replica database: %v", err)
			}
		}
	}
}
