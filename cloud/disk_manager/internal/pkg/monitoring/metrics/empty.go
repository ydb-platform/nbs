package metrics

import "github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"

////////////////////////////////////////////////////////////////////////////////

func NewEmptyRegistry() Registry {
	return empty.NewRegistry()
}
