package pig_test

import (
	"context"
	"testing"
	"time"

	"github.com/otetz/pig"
)

type key int

const expectedContextKey key = 123

func TestCtx(t *testing.T) {
	t.Parallel()

	expectedValue := 234

	ctx := context.Background()
	ctx = context.WithValue(ctx, expectedContextKey, expectedValue)

	var o pig.Options
	if o.Context != nil {
		t.Error(`should be nil`)
	}

	pig.Ctx(ctx)(&o)

	v := o.Context.Value(expectedContextKey)
	if v == nil {
		t.Fatal(`should not be nil`)
	}

	if v.(int) != expectedValue {
		t.Errorf(`should be %d, %d given`, expectedValue, v.(int))
	}
}

func TestTransactionTimeout(t *testing.T) {
	t.Parallel()

	var o pig.Options
	if o.TransactionTimeout != 0 {
		t.Errorf(`should be %d, %d given`, 0, o.TransactionTimeout)
	}

	pig.TransactionTimeout(time.Second)(&o)

	if o.TransactionTimeout != time.Second.Milliseconds() {
		t.Errorf(`should be %d, %d given`, time.Second.Milliseconds(), o.TransactionTimeout)
	}
}

func TestStatementTimeout(t *testing.T) {
	t.Parallel()

	var o pig.Options
	if o.StatementTimeout != 0 {
		t.Errorf(`should be %d, %d given`, 0, o.StatementTimeout)
	}

	pig.StatementTimeout(time.Second)(&o)

	if o.StatementTimeout != time.Second.Milliseconds() {
		t.Errorf(`should be %d, %d given`, time.Second.Milliseconds(), o.StatementTimeout)
	}
}
