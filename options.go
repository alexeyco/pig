package pig

import (
	"context"
	"time"
)

// Options query or tx options.
type Options struct {
	Context            context.Context
	TransactionTimeout int64
	StatementTimeout   int64
}

// Option func.
type Option func(*Options)

// Ctx sets query or tx context.
func Ctx(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

// TransactionTimeout sets transaction timeout (ignored with queries).
func TransactionTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.TransactionTimeout = d.Milliseconds()
	}
}

// StatementTimeout sets transaction statement timeout (ignored with queries).
func StatementTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.StatementTimeout = d.Milliseconds()
	}
}
