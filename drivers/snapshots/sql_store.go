package snapshots

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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
// ```sql
// CREATE TABLE IF NOT EXISTS "snapshots" (
//
//	"id" TEXT PRIMARY KEY,
//	"version" INTEGER NOT NULL,
//	"data" JSONB NOT NULL,
//	"timestamp" TIMESTAMP WITH TIME ZONE NOT NULL
//
// );
// ```
type SQLStore struct {
	DB        *sql.DB
	TableName string
}

// Load loads a snapshot from the store
func (ss SQLStore) Load(ctx context.Context, id string) (*sourcing.Snapshot, error) {
	if ss.TableName == "" {
		return nil, ErrTableNameNotSet
	}

	query := fmt.Sprintf("SELECT data, version, timestamp FROM %s WHERE id = $1", ss.TableName)
	snapshot := sourcing.Snapshot{}

	row := ss.DB.QueryRowContext(ctx, query, id)
	if err := row.Scan(&snapshot.Data, &snapshot.Version, &snapshot.Timestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, errors.Join(ErrLoadingSnapshot, err)
	}

	return &snapshot, nil
}

// Store stores a snapshot in the store
func (ss SQLStore) Store(ctx context.Context, aggregateID string, snapshot sourcing.Snapshot) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}
	query := fmt.Sprintf("INSERT INTO %s (id, version, data, timestamp) VALUES ($1, $2, $3, $4)", ss.TableName)
	_, err := ss.DB.ExecContext(ctx, query, aggregateID, snapshot.Version, snapshot.Data, snapshot.Timestamp)
	return err
}

// Delete deletes a snapshot from the store
func (ss SQLStore) Delete(ctx context.Context, aggregateID string) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}

	 query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", ss.TableName)
	_, err := ss.DB.ExecContext(ctx, query, aggregateID)
	return err
}
