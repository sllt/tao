package actor

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidRef means a string is not a valid local or node-qualified Ref.
var ErrInvalidRef = errors.New("actor: invalid reference")

// ParseRef accepts a path or node://node/path, matching Ref.String. Node names
// and path segments use the same limits and characters as Node and Spawn.
// It does not trim, unescape or normalize input, or accept URL credentials,
// ports, queries or fragments. An empty Node retains Ref's local/Directory
// routing semantics; parsing does not bind the address to a particular Node.
func ParseRef(address string) (Ref, error) {
	var ref Ref
	if rest, qualified := strings.CutPrefix(address, "node://"); qualified {
		node, path, ok := strings.Cut(rest, "/")
		if !ok {
			return Ref{}, fmt.Errorf("%w: expected node://node/path", ErrInvalidRef)
		}
		if !validSegment(node) {
			return Ref{}, fmt.Errorf("%w: invalid node name", ErrInvalidRef)
		}
		ref = Ref{Node: node, Path: path}
	} else {
		ref.Path = address
	}
	if !validPath(ref.Path) {
		return Ref{}, fmt.Errorf("%w: invalid path", ErrInvalidRef)
	}
	return ref, nil
}
