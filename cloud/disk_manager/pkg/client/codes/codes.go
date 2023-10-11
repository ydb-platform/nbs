package codes

import (
	"fmt"
)

type Code int64

const (
	OK                 = 0
	BadSource          = 1
	ResourceExhausted  = 2
	InvalidArgument    = 3
	PreconditionFailed = 4
	Aborted            = 5
)

func (c Code) String() string {
	switch c {
	case OK:
		return "OK"
	case BadSource:
		return "BadSource"
	case ResourceExhausted:
		return "ResourceExhausted"
	case InvalidArgument:
		return "InvalidArgument"
	case PreconditionFailed:
		return "PreconditionFailed"
	case Aborted:
		return "Aborted"
	default:
		return fmt.Sprintf("Code(%v)", int64(c))
	}
}
