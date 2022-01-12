package errorx

import (
	"fmt"
	"runtime"
	"strings"

	"manlu.org/tao/tools/taoctl/internal/version"
)

var errorFormat = `taoctl: generation error: %+v
taoctl version: %s
%s`

// TaoctlError represents a taoctl error.
type TaoctlError struct {
	message []string
	err     error
}

func (e *TaoctlError) Error() string {
	detail := wrapMessage(e.message...)
	v := fmt.Sprintf("%s %s/%s", version.BuildVersion, runtime.GOOS, runtime.GOARCH)
	return fmt.Sprintf(errorFormat, e.err, v, detail)
}

// Wrap wraps an error with taoctl version and message.
func Wrap(err error, message ...string) error {
	e, ok := err.(*TaoctlError)
	if ok {
		return e
	}

	return &TaoctlError{
		message: message,
		err:     err,
	}
}

func wrapMessage(message ...string) string {
	if len(message) == 0 {
		return ""
	}
	return fmt.Sprintf(`message: %s`, strings.Join(message, "\n"))
}