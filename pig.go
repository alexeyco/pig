// Package pig – simple pgx wrapper to execute and scan query results.
package pig

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Conn connection interface.
type Conn interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
}

// Handler to execute transaction.
type Handler func(*Ex) error

// Pig pgx wrapper.
type Pig struct {
	conn Conn
}

// Conn returns pgx connection.
func (p *Pig) Conn() Conn {
	return p.conn
}

// Query returns new query executor.
func (p *Pig) Query(options ...Option) *Ex {
	return &Ex{
		ex:      p.conn,
		options: p.options(options...),
	}
}

// Tx returns new transaction.
func (p *Pig) Tx(options ...Option) *Tx {
	return &Tx{
		conn:    p.conn,
		options: p.options(options...),
	}
}

func (p *Pig) options(options ...Option) Options {
	var o Options
	for _, opt := range options {
		opt(&o)
	}

	if o.Context == nil {
		o.Context = context.Background()
	}

	return o
}

// New returns new pig instance.
func New(conn Conn) *Pig {
	return &Pig{
		conn: conn,
	}
}
