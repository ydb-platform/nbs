package log

import (
	nbs "a.yandex-team.ru/cloud/blockstore/public/sdk/go/client"
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type WithLog struct {
	Log nbs.Log
}

func (x *WithLog) WriteLog(
	ctx context.Context,
	level nbs.LogLevel,
	fmt string,
	args ...interface{},
) {
	if logger := x.Log.Logger(level); logger != nil {
		logger.Printf(ctx, fmt, args...)
	}
}

func (x *WithLog) LogDbg(
	ctx context.Context,
	fmt string,
	args ...interface{},
) {
	x.WriteLog(ctx, nbs.LOG_DEBUG, fmt, args...)
}

func (x *WithLog) LogInfo(
	ctx context.Context,
	fmt string,
	args ...interface{},
) {
	x.WriteLog(ctx, nbs.LOG_INFO, fmt, args...)
}

func (x *WithLog) LogWarn(
	ctx context.Context,
	fmt string,
	args ...interface{},
) {
	x.WriteLog(ctx, nbs.LOG_WARN, fmt, args...)
}

func (x *WithLog) LogError(
	ctx context.Context,
	fmt string,
	args ...interface{},
) {
	x.WriteLog(ctx, nbs.LOG_ERROR, fmt, args...)
}
