package actor

import (
	"context"
	"fmt"
	"reflect"
)

// Call synchronously requests a typed reply. It honors caller and Actor
// cancellation, self-call checks and address routing. A reply that
// cannot be assigned to T returns its zero value and an ErrReplyType error.
// A nil reply is accepted for nil-capable T, matching Node.Call[T].
func (a *Actor) Call[T any](ctx context.Context, to Addr, msg any) (T, error) {
	var zero T
	value, err := a.call(ctx, to, msg)
	if err != nil {
		return zero, err
	}
	if value == nil {
		switch reflect.TypeFor[T]().Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			return zero, nil
		}
	}
	result, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf("%w: got %T", ErrReplyType, value)
	}
	return result, nil
}
