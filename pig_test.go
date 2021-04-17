package pig_test

import (
	"context"
	"testing"

	"github.com/alexeyco/pig"
	"github.com/pashagolub/pgxmock"
	"github.com/pkg/errors"
)

func connect(t *testing.T) pgxmock.PgxConnIface {
	t.Helper()

	conn, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf(`error should be nil, "%v" given`, err)
	}

	return conn
}

func TestPig_Query(t *testing.T) {
	t.Parallel()

	t.Run("Ok", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectExec("DELETE FROM table WHERE id = $1").
			WithArgs(123).
			WillReturnResult(pgxmock.NewResult("", 1))

		rowsAffected, err := pig.New(conn).
			Query().
			Exec("DELETE FROM table WHERE id = $1", 123)
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}

		if rowsAffected != 1 {
			t.Errorf(`should be %d, %d given`, 1, rowsAffected)
		}
	})

	t.Run("Error", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		expectedErr := errors.New("i am error")

		conn.ExpectExec("DELETE FROM table WHERE id = $1").
			WillReturnError(expectedErr)

		rowsAffected, err := pig.New(conn).
			Query().
			Exec("DELETE FROM table WHERE id = $1", 123)

		if err == nil {
			t.Fatal(`should not be nil`)
		}

		if rowsAffected != 0 {
			t.Errorf(`should be %d, %d given`, 0, rowsAffected)
		}
	})
}

func TestPig_Tx(t *testing.T) {
	t.Parallel()

	t.Run("Ok", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectBegin()
		conn.ExpectExec("DELETE FROM table WHERE id = $1").
			WithArgs(123).
			WillReturnResult(pgxmock.NewResult("", 1))
		conn.ExpectCommit()

		err := pig.New(conn).
			Tx().
			Exec(func(ex *pig.Ex) error {
				_, err := ex.Exec("DELETE FROM table WHERE id = $1", 123)

				// nolint:wrapcheck
				return err
			})
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}
	})
}
