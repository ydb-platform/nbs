CHECK_DEPENDENT_DIRS(
    DENY ALL
        contrib/ydb

    EXCEPT contrib/ydb/library/actors
    EXCEPT contrib/ydb/library/services
    EXCEPT contrib/ydb/library/yql/providers/generic/connector/api/common
    EXCEPT contrib/ydb/library/yql/public/types

    EXCEPT contrib/ydb/public/lib/protobuf # base for validation and config_proto_plugin
    EXCEPT contrib/ydb/public/lib/validation

    EXCEPT contrib/ydb/core/config
    EXCEPT contrib/ydb/core/protos/nbs

    EXCEPT GLOB contrib/ydb/**/proto
    EXCEPT GLOB contrib/ydb/**/protos
    EXCEPT GLOB contrib/ydb/**/protos/annotations
    EXCEPT GLOB contrib/ydb/**/protos/schemeshard
)
