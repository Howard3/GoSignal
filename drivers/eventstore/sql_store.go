package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Howard3/gosignal"
	"github.com/Howard3/gosignal/sourcing"
)

// ErrTableNameNotSet is the error returned when the table name is not set
var ErrTableNameNotSet = errors.New("table name not set")

// SQLStore is an opinionated store that uses a SQL database to store events
// it mirrors the gosignal.Event struct for fields when storing the events. Only ony table must
// be used per aggregate type as there is no expectation of columns for the aggregate type.
//
// it should use a schema that matches the following:
// ```sql
// CREATE TABLE events (
//
//	id SERIAL PRIMARY KEY,
//	type VARCHAR(255) NOT NULL,
//	data BYTEA NOT NULL,
//	version INT NOT NULL,
//	timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
//	aggregate_id VARCHAR(255) NOT NULL
//
// );
// ```
type SQLStore struct {
	DB        *sql.DB
	TableName string
}

// Store stores a list of events for a given aggregate id
func (ss SQLStore) Store(ctx context.Context, events []gosignal.Event) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (type, data, version, timestamp, aggregate_id) 
		VALUES ($1, $2, $3, $4, $5)`,
		ss.TableName)

	tx, err := ss.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, event := range events {
		_, err := tx.ExecContext(ctx, query, event.Type, event.Data, event.Version, event.Timestamp, event.AggregateID)
		if err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}

	return tx.Commit()
}

// Load loads all events for a given aggregate id
func (ss SQLStore) Load(ctx context.Context, aggID string, options sourcing.LoadEventsOptions) (evt []gosignal.Event, err error) {
	if ss.TableName == "" {
		return nil, ErrTableNameNotSet
	}

	query := fmt.Sprintf("SELECT type, data, version, timestamp FROM %s WHERE aggregate_id = $1", ss.TableName)
	rows, err := ss.DB.QueryContext(ctx, query, aggID)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Err())
	}()

	var events []gosignal.Event
	for rows.Next() {
		var event gosignal.Event
		if err := rows.Scan(&event.Type, &event.Data, &event.Version, &event.Timestamp); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

// Replace replaces an event with a new version, this mostly exists for legal compliance
// purposes, your event store should be append-only
func (ss SQLStore) Replace(ctx context.Context, id string, version uint, event gosignal.Event) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}

	query := fmt.Sprintf("UPDATE %s SET type = $1, data = $2, version = $3, timestamp = $4 WHERE id = $5", ss.TableName)
	_, err := ss.DB.ExecContext(ctx, query, event.Type, event.Data, event.Version, event.Timestamp, id)
	return err
}
