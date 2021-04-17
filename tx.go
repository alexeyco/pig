package pig

import (
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

const (
	transactionTimeoutQuery = "SET local idle_in_transaction_session_timeout = $1"
	statementTimeoutQuery   = "SET local statement_timeout = $1"
)

// Tx transaction.
type Tx struct {
	conn    Conn
	options Options
}

// Exec to execute transaction.
func (tx *Tx) Exec(handler Handler) error {
	err := tx.conn.BeginFunc(tx.options.Context, func(txx pgx.Tx) error {
		if tx.options.TransactionTimeout > 0 {
			if _, err := txx.Exec(tx.options.Context, transactionTimeoutQuery, tx.options.TransactionTimeout); err != nil {
				return errors.Wrap(err, "pig: set transaction timeout")
			}
		}

		if tx.options.StatementTimeout > 0 {
			if _, err := txx.Exec(tx.options.Context, statementTimeoutQuery, tx.options.StatementTimeout); err != nil {
				return errors.Wrap(err, "pig: set statement timeout")
			}
		}

		return handler(&Ex{
			ex:      txx,
			options: tx.options,
		})
	})

	return errors.WithStack(err)
}
