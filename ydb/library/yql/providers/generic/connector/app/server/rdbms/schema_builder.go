package rdbms

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/nbs/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/nbs/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"github.com/ydb-platform/nbs/library/go/core/log"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type schemaItem struct {
	columnName string
	columnType string
	ydbColumn  *Ydb.Column
}

type schemaBuilder struct {
	typeMapper          utils.TypeMapper
	typeMappingSettings *api_service_protos.TTypeMappingSettings
	items               []*schemaItem
}

func (sb *schemaBuilder) addColumn(columnName, columnType string) error {
	item := &schemaItem{
		columnName: columnName,
		columnType: columnType,
	}

	var err error
	item.ydbColumn, err = sb.typeMapper.SQLTypeToYDBColumn(columnName, columnType, sb.typeMappingSettings)

	if err != nil && !errors.Is(err, utils.ErrDataTypeNotSupported) {
		return fmt.Errorf("sql type to ydb column (%s, %s): %w", columnName, columnType, err)
	}

	sb.items = append(sb.items, item)

	return nil
}

func (sb *schemaBuilder) build(logger log.Logger) (*api_service_protos.TSchema, error) {
	if len(sb.items) == 0 {
		return nil, utils.ErrTableDoesNotExist
	}

	var (
		schema      api_service_protos.TSchema
		unsupported []string
	)

	for _, item := range sb.items {
		if item.ydbColumn == nil {
			unsupported = append(unsupported, fmt.Sprintf("%s %s", item.columnName, item.columnType))
		} else {
			schema.Columns = append(schema.Columns, item.ydbColumn)
		}
	}

	if len(unsupported) > 0 {
		logger.Warn(
			"the table schema was reduced because some column types are unsupported",
			log.Strings("unsupported columns", unsupported),
		)
	}

	return &schema, nil
}
