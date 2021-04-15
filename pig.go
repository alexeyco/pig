package pig

import (
	"context"

	"github.com/jackc/pgx/v4"
)

// Handler to execute transaction.
type Handler func(*Executor) error

// Pig pgx wrapper.
type Pig struct {
	conn *pgx.Conn
}

// Conn returns pgx connection.
func (p *Pig) Conn() *pgx.Conn {
	return p.conn
}

// Query returns new query executor.
func (p *Pig) Query(ctx context.Context) *Executor {
	return &Executor{
		ex:  p.conn,
		ctx: ctx,
	}
}

// Tx returns new transaction.
func (p *Pig) Tx(ctx context.Context) *Tx {
	return &Tx{
		pig: p,
		ctx: ctx,
	}
}

// New returns new pig instance.
func New(conn *pgx.Conn) *Pig {
	return &Pig{
		conn: conn,
	}
}
