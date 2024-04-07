package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Howard3/gosignal"
	"github.com/Howard3/gosignal/sourcing"
)

// ErrTableNameNotSet is the error returned when the table name is not set
var ErrTableNameNotSet = errors.New("table name not set")

type conditionBuilder struct {
	args []string
	opts []interface{}
	pph  func(int) string
}

func (cb *conditionBuilder) add(arg string, opt interface{}) {
	cb.args = append(cb.args, arg)
	cb.opts = append(cb.opts, opt)
}

// addIfNotNil adds a condition if the value is not nil
func (cb *conditionBuilder) addIfNotNil(arg string, opt interface{}) {
	if !reflect.ValueOf(opt).IsNil() { // don't know the interface, so we have to use reflection
		cb.add(arg, opt)
	}
}

func (cb *conditionBuilder) build() string {
	conditions := ""

	for i, v := range cb.args {
		if i == 0 {
			conditions += " WHERE "
		} else {
			conditions += " AND "
		}
		conditions += fmt.Sprintf("%s %s", v, cb.pph(i+1))
	}

	return conditions
}

// SQLStore is an opinionated store that uses a SQL database to store events
// it mirrors the gosignal.Event struct for fields when storing the events. Only ony table must
// be used per aggregate type as there is no expectation of columns for the aggregate type.
//
// it should use a schema that matches the following:
// ```sql
//
//	CREATE TABLE events (
//		id SERIAL PRIMARY KEY,
//		type VARCHAR(255) NOT NULL,
//		data BYTEA NOT NULL,
//		version INT NOT NULL,
//		timestamp INT NOT NULL,
//		aggregate_id VARCHAR(255) NOT NULL
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

// Store stores a list of events for a given aggregate id
func (ss SQLStore) Store(ctx context.Context, events []gosignal.Event) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}

	placeholders := []string{ss.pph(1), ss.pph(2), ss.pph(3), ss.pph(4), ss.pph(5)}

	query := fmt.Sprintf(`
		INSERT INTO %s (type, data, version, timestamp, aggregate_id) 
		VALUES (%s)`,
		ss.TableName, strings.Join(placeholders, ", "))

	tx, err := ss.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, event := range events {
		eventTimestamp := event.Timestamp.Unix()

		_, err := tx.ExecContext(ctx, query, event.Type, event.Data, event.Version, eventTimestamp, event.AggregateID)
		if err != nil {
			err := fmt.Errorf("when trying to update aggregate %s with version %d: %w", event.AggregateID, event.Version, err)
			return errors.Join(err, tx.Rollback())
		}
	}

	return tx.Commit()
}

// Load loads all events for a given aggregate id
// TODO: support max version, event types, and to/from timestamps
func (ss SQLStore) Load(ctx context.Context, aggID string, options sourcing.LoadEventsOptions) (evt []gosignal.Event, err error) {
	if ss.TableName == "" {
		return nil, ErrTableNameNotSet
	}
	query := fmt.Sprintf(`SELECT type, data, version, timestamp FROM %s`, ss.TableName)

	pph := ss.PositionalPlaceholderFn
	if pph == nil {
		pph = PositionalPlaceholderDollarSign
	}

	cb := conditionBuilder{pph: pph}
	cb.add("aggregate_id =", aggID)
	cb.addIfNotNil("version >=", options.MinVersion)
	cb.addIfNotNil("version <=", options.MaxVersion)

	query += cb.build()

	rows, err := ss.DB.QueryContext(ctx, query, cb.opts...)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Err())
	}()

	var events []gosignal.Event
	for rows.Next() {
		var event gosignal.Event
		var timestamp int
		if err := rows.Scan(&event.Type, &event.Data, &event.Version, &timestamp); err != nil {
			return nil, err
		}

		event.Timestamp = time.Unix(int64(timestamp), 0)
		event.AggregateID = aggID

		events = append(events, event)
	}

	return events, nil
}

// Replace replaces an event with a new version, this mostly exists for legal compliance
// purposes, your event store should be append-only
func (ss SQLStore) Replace(ctx context.Context, id string, version uint64, event gosignal.Event) error {
	if ss.TableName == "" {
		return ErrTableNameNotSet
	}

	eventTimestamp := event.Timestamp.Unix()

	query := fmt.Sprintf("UPDATE %s SET type = %s, data = %s, version = %s, timestamp = %s WHERE id = %s",
		ss.TableName,
		ss.pph(1), ss.pph(2), ss.pph(3), ss.pph(4), ss.pph(5),
	)
	_, err := ss.DB.ExecContext(ctx, query, event.Type, event.Data, event.Version, eventTimestamp, id)
	return err
}
