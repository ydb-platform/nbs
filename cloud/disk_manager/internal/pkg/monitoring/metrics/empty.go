package metrics

import "github.com/ydb-platform/nbs/library/go/core/metrics/nop"

func NewEmptyRegistry() Registry {
	return new(nop.Registry)
}
