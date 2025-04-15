// transaction.go
package fsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// Tx represents a database transaction with enhanced features
type Tx struct {
	tx *sqlx.Tx
}

// TxOptions defines the options for transactions
type TxOptions struct {
	// Isolation level for the transaction
	Isolation sql.IsolationLevel
	// ReadOnly transaction doesn't allow writes
	ReadOnly bool
	// Deferrable affects constraints checking (PostgreSQL specific)
	Deferrable bool
	// MaxRetries defines how many times to retry on conflict
	MaxRetries int
}

// Default transaction options
var DefaultTxOptions = TxOptions{
	Isolation:  sql.LevelDefault,
	ReadOnly:   false,
	Deferrable: false,
	MaxRetries: 3,
}

// ErrTxDone is returned when attempting an operation on a completed transaction
var ErrTxDone = errors.New("transaction has already been committed or rolled back")

// ErrMaxRetriesExceeded is returned when transaction exceeds max retry attempts
var ErrMaxRetriesExceeded = errors.New("transaction max retries exceeded")

// Common recoverable error substrings
var (
	retryableErrors = []string{
		"deadlock detected",
		"serialize",
		"serialization",
		"conflict",
		"concurrent update",
		"could not serialize access",
		"deadlock",
		"lock wait timeout",
		"lock timeout",
		"connection reset",
	}
)

// BeginTx starts a new transaction with the default options
func BeginTx(ctx context.Context) (*Tx, error) {
	return BeginTxWithOptions(ctx, DefaultTxOptions)
}

// BeginTxWithOptions starts a new transaction with the specified options
func BeginTxWithOptions(ctx context.Context, opts TxOptions) (*Tx, error) {
	sqlOpts := &sql.TxOptions{
		Isolation: opts.Isolation,
		ReadOnly:  opts.ReadOnly,
	}
	
	tx, err := Db.BeginTxx(ctx, sqlOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	
	// Set deferrable mode for PostgreSQL if requested
	if opts.Deferrable {
		_, err = tx.Exec("SET CONSTRAINTS ALL DEFERRED")
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to set deferred constraints: %w", err)
		}
	}
	
	return &Tx{tx: tx}, nil
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.tx == nil {
		return ErrTxDone
	}
	
	err := tx.tx.Commit()
	tx.tx = nil
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	
	return nil
}

// Rollback aborts the transaction
func (tx *Tx) Rollback() error {
	if tx.tx == nil {
		return ErrTxDone
	}
	
	err := tx.tx.Rollback()
	tx.tx = nil
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	
	return nil
}

// Exec executes a query within the transaction
func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}
	
	// Get or create prepared statement for better performance
	stmt, err := tx.tx.Preparex(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement in transaction: %w", err)
	}
	defer stmt.Close()
	
	return stmt.Exec(args...)
}

// ExecContext executes a query with a context within the transaction
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}
	
	// Get or create prepared statement for better performance
	stmt, err := tx.tx.PreparexContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement in transaction: %w", err)
	}
	defer stmt.Close()
	
	return stmt.ExecContext(ctx, args...)
}

// Query executes a query that returns rows within the transaction
func (tx *Tx) Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}
	
	return tx.tx.Queryx(query, args...)
}

// QueryContext executes a query with context that returns rows within the transaction
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}
	
	return tx.tx.QueryxContext(ctx, query, args...)
}

// QueryRow executes a query that returns a single row within the transaction
func (tx *Tx) QueryRow(query string, args ...interface{}) *sqlx.Row {
	if tx.tx == nil {
		return nil
	}
	
	return tx.tx.QueryRowx(query, args...)
}

// QueryRowContext executes a query with context that returns a single row within the transaction
func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	if tx.tx == nil {
		return nil
	}
	
	return tx.tx.QueryRowxContext(ctx, query, args...)
}

// Get retrieves a single item from the database within the transaction
func (tx *Tx) Get(dest interface{}, query string, args ...interface{}) error {
	if tx.tx == nil {
		return ErrTxDone
	}
	
	// Use prepared statement for better performance
	stmt, err := tx.tx.Preparex(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement in transaction: %w", err)
	}
	defer stmt.Close()
	
	return stmt.Get(dest, args...)
}

// GetContext retrieves a single item with context from the database within the transaction
func (tx *Tx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if tx.tx == nil {
		return ErrTxDone
	}
	
	// Use prepared statement for better performance
	stmt, err := tx.tx.PreparexContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement in transaction: %w", err)
	}
	defer stmt.Close()
	
	return stmt.GetContext(ctx, dest, args...)
}

// Select retrieves multiple items from the database within the transaction
func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	if tx.tx == nil {
		return ErrTxDone
	}
	
	// Use prepared statement for better performance
	stmt, err := tx.tx.Preparex(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement in transaction: %w", err)
	}
	defer stmt.Close()
	
	return stmt.Select(dest, args...)
}

// SelectContext retrieves multiple items with context from the database within the transaction
func (tx *Tx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if tx.tx == nil {
		return ErrTxDone
	}
	
	// Use prepared statement for better performance
	stmt, err := tx.tx.PreparexContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement in transaction: %w", err)
	}
	defer stmt.Close()
	
	return stmt.SelectContext(ctx, dest, args...)
}

// NamedExec executes a named query within the transaction
func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}
	
	return tx.tx.NamedExec(query, arg)
}

// NamedExecContext executes a named query with context within the transaction
func (tx *Tx) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	if tx.tx == nil {
		return nil, ErrTxDone
	}
	
	return tx.tx.NamedExecContext(ctx, query, arg)
}

// TxFn defines a function that uses a transaction
type TxFn func(*Tx) error

// WithTx executes a function within a transaction
// If the function returns an error, the transaction is rolled back
// If the function returns nil, the transaction is committed
func WithTx(ctx context.Context, fn TxFn) error {
	return WithTxOptions(ctx, DefaultTxOptions, fn)
}

// WithTxOptions executes a function within a transaction with options
func WithTxOptions(ctx context.Context, opts TxOptions, fn TxFn) error {
	tx, err := BeginTxWithOptions(ctx, opts)
	if err != nil {
		return err
	}
	
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // Re-throw panic after rollback
		}
	}()
	
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}
	
	return tx.Commit()
}

// WithTxRetry executes a function within a transaction with retry logic
// If the function returns a retryable error, the transaction is retried
func WithTxRetry(ctx context.Context, fn TxFn) error {
	return WithTxRetryOptions(ctx, DefaultTxOptions, fn)
}

// WithTxRetryOptions executes a function within a transaction with retry logic and options
func WithTxRetryOptions(ctx context.Context, opts TxOptions, fn TxFn) error {
	var err error
	maxRetries := opts.MaxRetries
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		attemptErr := WithTxOptions(ctx, opts, fn)
		if attemptErr == nil {
			return nil
		}
		
		// Store the error for possible return
		err = attemptErr
		
		// Check if we should retry
		if !isRetryableError(err) {
			return err
		}
		
		// Wait with exponential backoff with jitter before retrying
		// Using 100ms base with exponential backoff and up to 20% jitter
		baseBackoff := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
		jitter := time.Duration(float64(baseBackoff) * 0.2 * (rand.Float64() - 0.5))
		backoff := baseBackoff + jitter
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Continue with retry
		}
	}
	
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, err)
	}
	
	return ErrMaxRetriesExceeded
}

// isRetryableError determines if an error can be retried
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	errMsg := err.Error()
	for _, retryMsg := range retryableErrors {
		if strings.Contains(strings.ToLower(errMsg), retryMsg) {
			return true
		}
	}
	
	return false
}

// WithReadTx executes a function within a read-only transaction
func WithReadTx(ctx context.Context, fn TxFn) error {
	opts := DefaultTxOptions
	opts.ReadOnly = true
	return WithTxOptions(ctx, opts, fn)
}

// WithSerializableTx executes a function within a serializable isolation level transaction
func WithSerializableTx(ctx context.Context, fn TxFn) error {
	opts := DefaultTxOptions
	opts.Isolation = sql.LevelSerializable
	return WithTxRetryOptions(ctx, opts, fn)
}

// WithReadCommittedTx executes a function within a read committed isolation level transaction
func WithReadCommittedTx(ctx context.Context, fn TxFn) error {
	opts := DefaultTxOptions
	opts.Isolation = sql.LevelReadCommitted
	return WithTxOptions(ctx, opts, fn)
}

// GetTxById returns a transaction by ID for compatibility with existing code
func GetTxById(id string) (*Tx, error) {
	// This function is a placeholder for compatibility with code that expects
	// to be able to retrieve a transaction by ID. Since our transactions aren't
	// stored by ID, this function always returns an error.
	return nil, errors.New("transaction retrieval by ID is not supported")
}