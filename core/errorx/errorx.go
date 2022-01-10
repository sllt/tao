package errorx

import (
	"fmt"
	"manlu.org/tao/core/errorx/code"
)

// iCode is the interface for Code feature.
type iCode interface {
	Error() string
	Code() code.Code
}

// iStack is the interface for Stack feature.
type iStack interface {
	Error() string
	Stack() string
}

// iCause is the interface for Cause feature.
type iCause interface {
	Error() string
	Cause() error
}

// iCurrent is the interface for Current feature.
type iCurrent interface {
	Error() string
	Current() error
}

// iNext is the interface for Next feature.
type iNext interface {
	Error() string
	Next() error
}

// New creates and returns an error which is formatted from given text.
func New(text string) error {
	return &Error{
		stack: callers(),
		text:  text,
		code:  code.CodeNil,
	}
}

// Newf returns an error that formats as the given format and args.
func Newf(format string, args ...interface{}) error {
	return &Error{
		stack: callers(),
		text:  fmt.Sprintf(format, args...),
		code:  code.CodeNil,
	}
}

// NewSkip creates and returns an error which is formatted from given text.
// The parameter `skip` specifies the stack callers skipped amount.
func NewSkip(skip int, text string) error {
	return &Error{
		stack: callers(skip),
		text:  text,
		code:  code.CodeNil,
	}
}

// NewSkipf returns an error that formats as the given format and args.
// The parameter `skip` specifies the stack callers skipped amount.
func NewSkipf(skip int, format string, args ...interface{}) error {
	return &Error{
		stack: callers(skip),
		text:  fmt.Sprintf(format, args...),
		code:  code.CodeNil,
	}
}

// Wrap wraps error with text.
// It returns nil if given err is nil.
func Wrap(err error, text string) error {
	if err == nil {
		return nil
	}
	return &Error{
		error: err,
		stack: callers(),
		text:  text,
		code:  Code(err),
	}
}

// Wrapf returns an error annotating err with a stack trace
// at the point Wrapf is called, and the format specifier.
// It returns nil if given `err` is nil.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &Error{
		error: err,
		stack: callers(),
		text:  fmt.Sprintf(format, args...),
		code:  Code(err),
	}
}

// WrapSkip wraps error with text.
// It returns nil if given err is nil.
// The parameter `skip` specifies the stack callers skipped amount.
func WrapSkip(skip int, err error, text string) error {
	if err == nil {
		return nil
	}
	return &Error{
		error: err,
		stack: callers(skip),
		text:  text,
		code:  Code(err),
	}
}

// WrapSkipf wraps error with text that is formatted with given format and args.
// It returns nil if given err is nil.
// The parameter `skip` specifies the stack callers skipped amount.
func WrapSkipf(skip int, err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &Error{
		error: err,
		stack: callers(skip),
		text:  fmt.Sprintf(format, args...),
		code:  Code(err),
	}
}

// NewCode creates and returns an error that has error code and given text.
func NewCode(code code.Code, text ...string) error {
	errText := ""
	if len(text) > 0 {
		errText = text[0]
	}
	return &Error{
		stack: callers(),
		text:  errText,
		code:  code,
	}
}

// NewCodef returns an error that has error code and formats as the given format and args.
func NewCodef(code code.Code, format string, args ...interface{}) error {
	return &Error{
		stack: callers(),
		text:  fmt.Sprintf(format, args...),
		code:  code,
	}
}

// NewCodeSkip creates and returns an error which has error code and is formatted from given text.
// The parameter `skip` specifies the stack callers skipped amount.
func NewCodeSkip(code code.Code, skip int, text ...string) error {
	errText := ""
	if len(text) > 0 {
		errText = text[0]
	}
	return &Error{
		stack: callers(skip),
		text:  errText,
		code:  code,
	}
}

// NewCodeSkipf returns an error that has error code and formats as the given format and args.
// The parameter `skip` specifies the stack callers skipped amount.
func NewCodeSkipf(code code.Code, skip int, format string, args ...interface{}) error {
	return &Error{
		stack: callers(skip),
		text:  fmt.Sprintf(format, args...),
		code:  code,
	}
}

// WrapCode wraps error with code and text.
// It returns nil if given err is nil.
func WrapCode(code code.Code, err error, text ...string) error {
	if err == nil {
		return nil
	}
	errText := ""
	if len(text) > 0 {
		errText = text[0]
	}
	return &Error{
		error: err,
		stack: callers(),
		text:  errText,
		code:  code,
	}
}

// WrapCodef wraps error with code and format specifier.
// It returns nil if given `err` is nil.
func WrapCodef(code code.Code, err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &Error{
		error: err,
		stack: callers(),
		text:  fmt.Sprintf(format, args...),
		code:  code,
	}
}

// WrapCodeSkip wraps error with code and text.
// It returns nil if given err is nil.
// The parameter `skip` specifies the stack callers skipped amount.
func WrapCodeSkip(code code.Code, skip int, err error, text ...string) error {
	if err == nil {
		return nil
	}
	errText := ""
	if len(text) > 0 {
		errText = text[0]
	}
	return &Error{
		error: err,
		stack: callers(skip),
		text:  errText,
		code:  code,
	}
}

// WrapCodeSkipf wraps error with code and text that is formatted with given format and args.
// It returns nil if given err is nil.
// The parameter `skip` specifies the stack callers skipped amount.
func WrapCodeSkipf(code code.Code, skip int, err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &Error{
		error: err,
		stack: callers(skip),
		text:  fmt.Sprintf(format, args...),
		code:  code,
	}
}

// Code returns the error code of current error.
// It returns CodeNil if it has no error code or it does not implements interface Code.
func Code(err error) code.Code {
	if err != nil {
		if e, ok := err.(iCode); ok {
			return e.Code()
		}
	}
	return code.CodeNil
}

// Cause returns the root cause error of `err`.
func Cause(err error) error {
	if err != nil {
		if e, ok := err.(iCause); ok {
			return e.Cause()
		}
	}
	return err
}

// Stack returns the stack callers as string.
// It returns the error string directly if the `err` does not support stacks.
func Stack(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := err.(iStack); ok {
		return e.Stack()
	}
	return err.Error()
}

// Current creates and returns the current level error.
// It returns nil if current level error is nil.
func Current(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(iCurrent); ok {
		return e.Current()
	}
	return err
}

// Next returns the next level error.
// It returns nil if current level error or the next level error is nil.
func Next(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(iNext); ok {
		return e.Next()
	}
	return nil
}

// HasStack checks and returns whether `err` implemented interface `iStack`.
func HasStack(err error) bool {
	_, ok := err.(iStack)
	return ok
}
