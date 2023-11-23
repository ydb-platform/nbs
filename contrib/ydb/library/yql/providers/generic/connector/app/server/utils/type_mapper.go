package utils

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	api_service_protos "github.com/ydb-platform/nbs/contrib/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type TypeMapper interface {
	SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error)

	AddRowToArrowIPCStreaming(
		ydbTypes []*Ydb.Type,
		acceptors []any,
		builders []array.Builder) error
}
