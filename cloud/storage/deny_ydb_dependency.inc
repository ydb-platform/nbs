CHECK_DEPENDENT_DIRS(
    DENY ALL
        ydb

    EXCEPT ydb/library/actors
    EXCEPT ydb/library/services
    EXCEPT ydb/library/yql/providers/generic/connector/api/common
    EXCEPT ydb/library/yql/public/types

    EXCEPT ydb/public/lib/protobuf # base for validation and config_proto_plugin
    EXCEPT ydb/public/lib/validation

    EXCEPT ydb/core/config
    EXCEPT ydb/core/protos/nbs

    EXCEPT GLOB ydb/**/proto
    EXCEPT GLOB ydb/**/protos
    EXCEPT GLOB ydb/**/protos/annotations
    EXCEPT GLOB ydb/**/protos/schemeshard
)
