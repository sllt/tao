package taonet

import (
	"context"
	"errors"
	"testing"
	"time"

	native "github.com/sllt/tao/taonet/internal/mux"
)

// A deadline can pass before its context timer runs. Model that window without
// relying on scheduler load or a sleep racing a real context callback.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx delayedDeadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

func TestOperationErrorExpiredBudgetBeforeContextSignal(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name  string
		ctx   context.Context
		code  Code
		cause error
	}{
		{"late-timer", delayedDeadlineContext{context.Background(), time.Now().Add(-time.Second)}, CodeDeadlineExceeded, context.DeadlineExceeded},
		{"explicit-cancel-won", delayedDeadlineContext{canceled, time.Now().Add(-time.Second)}, CodeCanceled, context.Canceled},
		{"live-budget", delayedDeadlineContext{context.Background(), time.Now().Add(time.Hour)}, CodeCanceled, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			reset := &native.StreamError{Code: native.Canceled}
			err := operationError(test.ctx, reset, Unknown, 42)
			requireOpError(t, err, test.code, Unknown)
			var op *OpError
			if !errors.As(err, &op) || op.Operation != 42 {
				t.Fatalf("lost operation identity: %v", err)
			}
			if test.cause != nil && !errors.Is(err, test.cause) {
				t.Fatalf("wrong cancellation cause: %v", err)
			}
			if test.cause == nil && !errors.Is(err, reset) {
				t.Fatalf("live budget replaced the peer reset: %v", err)
			}
		})
	}
}
