package rdbms

import (
	"context"

	api_common "github.com/ydb-platform/nbs/contrib/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/nbs/contrib/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/nbs/contrib/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/nbs/contrib/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"github.com/ydb-platform/nbs/library/go/core/log"
)

type Handler interface {
	DescribeTable(
		ctx context.Context,
		logger log.Logger,
		request *api_service_protos.TDescribeTableRequest,
	) (*api_service_protos.TDescribeTableResponse, error)

	ReadSplit(
		ctx context.Context,
		logger log.Logger,
		split *api_service_protos.TSplit,
		sink paging.Sink,
	)

	TypeMapper() utils.TypeMapper
}

type HandlerFactory interface {
	Make(
		logger log.Logger,
		dataSourceType api_common.EDataSourceKind,
	) (Handler, error)
}
