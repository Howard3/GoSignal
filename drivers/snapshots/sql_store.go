package snapshots

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Howard3/gosignal/sourcing"
)

// ErrTableNameNotSet is returned when the table name is not set
var ErrTableNameNotSet = fmt.Errorf("table name not set")

// ErrLoadingSnapshot is returned when a snapshot cannot be loaded
var ErrLoadingSnapshot = fmt.Errorf("error loading snapshot")

// SQLStore is a store for snapshots that uses a SQL database as its backend.
// it is highly opinionated and expects there to be columns matching the sourcing.Snapshot struct
// that means there will be a column for the id, version, and data, and timestamp
// Further, every aggregate must have its own table as there is no "aggregate type" column
// it should use a schema that matches the following:
//
// ```sql
//
//	CREATE TABLE IF NOT EXISTS "snapshots" (
//		"id" TEXT PRIMARY KEY,
//		"version" INTEGER NOT NULL,
//		"data" JSONB NOT NULL,
//		"timestamp" INT NOT NULL
//	);
//
// ```
type SQLStore struct {
	DB                      *sql.DB
	TableName               string
	PositionalPlaceholderFn func(int) string
}

func PositionalPlaceholderDollarSign(i int) string {
	return fmt.Sprintf("$%d", i)
}

func (ss SQLStore) pph(i int) string {
	if ss.PositionalPlaceholderFn != nil {
		return ss.PositionalPlaceholderFn(i)
	}
	return PositionalPlaceholderDollarSign(i)
}

// Load loads a snapshot from the store
func (ss SQLStore) Load(ctx context.Context, id string) (*sourcing.Snapshot, error) {
	if ss.TableName == "" {
		return nil, ErrTableNameNotSet
	}

	query := fmt.Sprintf("SELECT data, version, timestamp FROM %s WHERE id = %s", ss.TableName, ss.pph(1))
	snapshot := sourcing.Snapshot{ID: id}
	var timestamp int

	row := ss.DB.QueryRowContext(ctx, query, id)
	if err := row.Scan(&snapshot.Data, &snapshot.Version, &timestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, errors.Join(ErrLoadingSnapshot, err)
	}

	snapshot.Timestamp = time.Unix(int64(timestamp), 0)

	return &snapshot, nil
}

// Store stores a snapshot in the store
func (ss SQLStore) Store(ctx context.Context, aggregateID string, snapshot sourcing.Snapshot) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}

	ssTimestamp := snapshot.Timestamp.Unix()

	query := fmt.Sprintf(`INSERT INTO %s (id, version, data, timestamp) 
		VALUES (%s)
		ON CONFLICT (id) DO UPDATE SET version = %s, data = %s, timestamp = %s 
	`, ss.TableName,
		strings.Join([]string{ss.pph(1), ss.pph(2), ss.pph(3), ss.pph(4)}, ", "),
		ss.pph(2), ss.pph(3), ss.pph(4),
	)
	_, err := ss.DB.ExecContext(ctx, query, aggregateID, snapshot.Version, snapshot.Data, ssTimestamp)
	return err
}

// Delete deletes a snapshot from the store
func (ss SQLStore) Delete(ctx context.Context, aggregateID string) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id = %s", ss.TableName, ss.pph(1))
	_, err := ss.DB.ExecContext(ctx, query, aggregateID)
	return err
}
