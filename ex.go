package pig

import (
	"context"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
)

type executable interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// Ex to execute queries.
type Ex struct {
	ex      executable
	options Options
}

// Exec query and return affected rows.
func (e *Ex) Exec(sql string, args ...interface{}) (int64, error) {
	t, err := e.ex.Exec(e.options.Context, sql, args...)
	if err != nil {
		return 0, errors.Wrap(err, "pig: execute query")
	}

	return t.RowsAffected(), nil
}

// Get single record.
func (e *Ex) Get(dst interface{}, sql string, args ...interface{}) error {
	rows, err := e.ex.Query(e.options.Context, sql, args...)
	if err != nil {
		return errors.Wrap(err, "pig: get one result row")
	}

	err = pgxscan.ScanOne(dst, rows)

	return errors.WithStack(err)
}

// Select multiple records.
func (e *Ex) Select(dst interface{}, sql string, args ...interface{}) error {
	rows, err := e.ex.Query(e.options.Context, sql, args...)
	if err != nil {
		return errors.Wrap(err, "pig: select multiple result row")
	}

	err = pgxscan.ScanAll(dst, rows)

	return errors.WithStack(err)
}
