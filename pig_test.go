package pig_test

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/pkg/errors"

	"github.com/otetz/pig"
)

func connect(t *testing.T) pgxmock.PgxConnIface {
	t.Helper()

	conn, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf(`error should be nil, "%v" given`, err)
	}

	conn.MatchExpectationsInOrder(true)

	return conn
}

type thing struct {
	ID       int64  `db:"id"`
	Name     string `db:"name"`
	Quantity int64  `db:"quantity"`
}

func (t thing) String() string {
	return fmt.Sprintf(`{ID: %d, Name: "%s", Quantity: %d}`, t.ID, t.Name, t.Quantity)
}

func (t thing) isZero() bool {
	return t.ID == 0 && t.Name == "" && t.Quantity == 0
}

type things []thing

func (t things) String() string {
	parts := make([]string, len(t))
	for n, th := range t {
		parts[n] = th.String()
	}

	return strings.Join(parts, ", ")
}

var errExpected = errors.New("i am error")

func TestPig_Conn(t *testing.T) {
	t.Parallel()

	conn := connect(t)
	defer func() { _ = conn.Close(context.Background()) }()

	if !reflect.DeepEqual(pig.New(conn).Conn(), conn) {
		t.Fatal(`should be equal`)
	}
}

func TestPig_Query(t *testing.T) {
	t.Parallel()

	t.Run("ExecOk", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectExec("DELETE FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		rowsAffected, err := pig.New(conn).
			Query().
			Exec("DELETE FROM things WHERE id = $1", 123)
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}

		if rowsAffected != 1 {
			t.Errorf(`should be %d, %d given`, 1, rowsAffected)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}
	})

	t.Run("ExecFailed", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectExec("DELETE FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnError(errExpected)

		rowsAffected, err := pig.New(conn).
			Query().
			Exec("DELETE FROM things WHERE id = $1", 123)

		if err == nil {
			t.Fatal(`should not be nil`)
		}

		if rowsAffected != 0 {
			t.Errorf(`should be %d, %d given`, 0, rowsAffected)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}
	})

	t.Run("GetOk", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		rows := conn.NewRows([]string{"id", "name", "quantity"}).
			AddRow(int64(123), "Some thing", int64(456))

		conn.ExpectQuery("SELECT * FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnRows(rows)

		var actual thing
		err := pig.New(conn).
			Query().
			Get(&actual, "SELECT * FROM things WHERE id = $1", 123)
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}

		expected := thing{
			ID:       123,
			Name:     "Some thing",
			Quantity: 456,
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf(`result should be %s, %s given`, expected, actual)
		}
	})

	t.Run("GetFailed", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectQuery("SELECT * FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnError(errExpected)

		var actual thing
		err := pig.New(conn).
			Query().
			Get(&actual, "SELECT * FROM things WHERE id = $1", 123)
		if err == nil {
			t.Fatal(`should not be nil`)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}

		if !actual.isZero() {
			t.Errorf(`result should be empty, %s given`, actual)
		}
	})

	t.Run("SelectOk", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		rows := conn.NewRows([]string{"id", "name", "quantity"}).
			AddRow(int64(123), "Some thing1", int64(456)).
			AddRow(int64(789), "Some thing2", int64(123))

		conn.ExpectQuery("SELECT * FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnRows(rows)

		var actual things
		err := pig.New(conn).
			Query().
			Select(&actual, "SELECT * FROM things WHERE id = $1", 123)
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}

		expected := things{
			{
				ID:       123,
				Name:     "Some thing1",
				Quantity: 456,
			},
			{
				ID:       789,
				Name:     "Some thing2",
				Quantity: 123,
			},
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf(`result should be %s, %s given`, expected, actual)
		}
	})

	t.Run("SelectFailed", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectQuery("SELECT * FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnError(errExpected)

		var actual things
		err := pig.New(conn).
			Query().
			Select(&actual, "SELECT * FROM things WHERE id = $1", 123)
		if err == nil {
			t.Fatal(`should not be nil`)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}

		if len(actual) != 0 {
			t.Errorf(`result should be empty, %s given`, actual)
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
		conn.ExpectExec("DELETE FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnResult(pgxmock.NewResult("DELETE", 1))
		conn.ExpectRollback()

		err := pig.New(conn).
			Tx().
			Exec(func(ex *pig.Ex) error {
				_, err := ex.Exec("DELETE FROM things WHERE id = $1", 123)

				return err
			})
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}
	})

	t.Run("Failed", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectBegin()
		conn.ExpectExec("DELETE FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnError(errExpected)
		conn.ExpectRollback()

		err := pig.New(conn).
			Tx().
			Exec(func(ex *pig.Ex) error {
				_, err := ex.Exec("DELETE FROM things WHERE id = $1", 123)

				return err
			})
		if err == nil {
			t.Fatal(`should not be nil`)
		}

		if !errors.Is(err, errExpected) {
			t.Errorf(`should be "%v", "%v" given`, errExpected, err)
		}
	})

	t.Run("StatementTimeoutOk", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectBegin()
		conn.ExpectExec(`SET local statement_timeout = $1`).
			WithArgs(int64(1000)).
			WillReturnResult(pgxmock.NewResult("SET", 1))
		conn.ExpectExec("DELETE FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnResult(pgxmock.NewResult("DELETE", 1))
		conn.ExpectRollback()

		err := pig.New(conn).
			Tx(pig.StatementTimeout(time.Second)).
			Exec(func(ex *pig.Ex) error {
				_, err := ex.Exec("DELETE FROM things WHERE id = $1", 123)

				return err
			})
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}
	})

	t.Run("StatementTimeoutFailed", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectBegin()
		conn.ExpectExec(`SET local statement_timeout = $1`).
			WithArgs(int64(1000)).
			WillReturnError(errExpected)
		conn.ExpectRollback()
		conn.ExpectRollback()

		err := pig.New(conn).
			Tx(pig.StatementTimeout(time.Second)).
			Exec(func(ex *pig.Ex) error {
				_, err := ex.Exec("DELETE FROM things WHERE id = $1", 123)

				return err
			})
		if err == nil {
			t.Fatal(`should not be nil`)
		}

		if !errors.Is(err, errExpected) {
			t.Errorf(`should be "%v", "%v" given`, errExpected, err)
		}
	})

	t.Run("TransactionTimeoutOk", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectBegin()
		conn.ExpectExec(`SET local idle_in_transaction_session_timeout = $1`).
			WithArgs(int64(1000)).
			WillReturnResult(pgxmock.NewResult("SET", 1))
		conn.ExpectExec("DELETE FROM things WHERE id = $1").
			WithArgs(123).
			WillReturnResult(pgxmock.NewResult("DELETE", 1))
		conn.ExpectRollback()

		err := pig.New(conn).
			Tx(pig.TransactionTimeout(time.Second)).
			Exec(func(ex *pig.Ex) error {
				_, err := ex.Exec("DELETE FROM things WHERE id = $1", 123)

				return err
			})
		if err != nil {
			t.Fatalf(`should be nil, "%v" given`, err)
		}

		if err = conn.ExpectationsWereMet(); err != nil {
			t.Errorf(`there were unfulfilled expectations: %v`, err)
		}
	})

	t.Run("TransactionTimeoutFailed", func(t *testing.T) {
		t.Parallel()

		conn := connect(t)
		defer func() { _ = conn.Close(context.Background()) }()

		conn.ExpectBegin()
		conn.ExpectExec(`SET local idle_in_transaction_session_timeout = $1`).
			WithArgs(int64(1000)).
			WillReturnError(errExpected)
		conn.ExpectRollback()
		conn.ExpectRollback()

		err := pig.New(conn).
			Tx(pig.TransactionTimeout(time.Second)).
			Exec(func(ex *pig.Ex) error {
				_, err := ex.Exec("DELETE FROM things WHERE id = $1", 123)

				return err
			})
		if err == nil {
			t.Fatal(`should not be nil`)
		}

		if !errors.Is(err, errExpected) {
			t.Errorf(`should be "%v", "%v" given`, errExpected, err)
		}
	})
}

func ExamplePig_Query() {
	conn, err := pgx.Connect(context.Background(), "")
	if err != nil {
		log.Fatalln(err)
	}

	p := pig.New(conn)

	// Execute query
	affectedRows, err := p.Query().Exec("DELETE FROM things WHERE id = $1", 123)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("affected", affectedRows, "rows")

	// Get single record from database
	var cnt int64
	err = p.Query().Get(&cnt, "SELECT count(*) FROM things")
	if err != nil {
		log.Fatalln(err)
	}

	type Thing struct {
		ID       int64  `db:"id"`
		Name     string `db:"name"`
		Quantity int64  `db:"quantity"`
	}

	// Select multiple records
	var things []Thing
	err = p.Query().Select(&things, "SELECT * FROM things")
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(things)
}

func ExamplePig_Tx() {
	conn, err := pgx.Connect(context.Background(), "")
	if err != nil {
		log.Fatalln(err)
	}

	p := pig.New(conn)

	var affectedRows int64
	err = p.Tx().Exec(func(ex *pig.Ex) error {
		affectedRows, err = p.Query().Exec("DELETE FROM things WHERE id = $1", 123)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("affected", affectedRows, "rows")
}
