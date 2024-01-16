package logging

import (
	"github.com/ydb-platform/nbs/library/go/core/log/zap"
)

////////////////////////////////////////////////////////////////////////////////

func NewStderrLogger(level Level) Logger {
	return zap.Must(zap.ConsoleConfig(level))
}
