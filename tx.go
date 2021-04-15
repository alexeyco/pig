package pig

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

const (
	transactionTimeoutQuery = "SET local idle_in_transaction_session_timeout = $1"
	statementTimeoutQuery   = "SET local statement_timeout = $1"
)

// Tx transaction.
type Tx struct {
	pig                *Pig
	ctx                context.Context
	transactionTimeout time.Duration
	statementTimeout   time.Duration
}

// TransactionTimeout sets local idle_in_transaction_session_timeout option.
func (tx *Tx) TransactionTimeout(d time.Duration) *Tx {
	tx.transactionTimeout = d
	return tx
}

// StatementTimeout sets local statement_timeout option.
func (tx *Tx) StatementTimeout(d time.Duration) *Tx {
	tx.statementTimeout = d
	return tx
}

// Execute transaction.
func (tx *Tx) Execute(handler Handler) error {
	err := tx.pig.conn.BeginFunc(tx.ctx, func(txx pgx.Tx) error {
		if tx.transactionTimeout.Milliseconds() > 0 {
			if _, err := txx.Exec(tx.ctx, transactionTimeoutQuery, tx.transactionTimeout.Milliseconds()); err != nil {
				return errors.Wrap(err, "pig: set transaction timeout")
			}
		}

		if tx.statementTimeout.Milliseconds() > 0 {
			if _, err := txx.Exec(tx.ctx, statementTimeoutQuery, tx.statementTimeout.Milliseconds()); err != nil {
				return errors.Wrap(err, "pig: set statement timeout")
			}
		}

		err := handler(&Executor{
			ex:  txx,
			ctx: tx.ctx,
		})

		return errors.WithStack(err)
	})

	return errors.WithStack(err)
}
