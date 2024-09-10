CHECK_DEPENDENT_DIRS(
    DENY ALL
        ydb

    EXCEPT ydb/library/actors
    EXCEPT ydb/library/services
    EXCEPT ydb/library/yql/providers/generic/connector/api/common
    EXCEPT ydb/library/yql/public/types
    EXCEPT ydb/public/lib/validation

    EXCEPT ydb/public/lib/protobuf # base for validation and config_proto_plugin
    EXCEPT ydb/core/config/tools/protobuf_plugin/config_proto_plugin

    EXCEPT GLOB ydb/**/proto
    EXCEPT GLOB ydb/**/protos
    EXCEPT GLOB ydb/**/protos/annotations
)
