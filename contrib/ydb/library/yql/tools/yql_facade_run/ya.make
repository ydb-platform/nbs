LIBRARY()

SRCS(
    yql_facade_run.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/pg/provider
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/common/gateways_utils
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/core/file_storage/defs
    contrib/ydb/library/yql/core/url_lister/interface
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/core/credentials
    contrib/ydb/library/yql/core/pg_ext
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/url_lister
    contrib/ydb/library/yql/core/url_preprocessing
    contrib/ydb/library/yql/core/peephole_opt
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/core/qplayer/storage/file
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/result_format
    contrib/ydb/library/yql/utils/failure_injector
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1/ide/completion/check
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/format/check
    contrib/ydb/library/yql/sql/v1/lexer/check
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/public/langver
    contrib/ydb/library/yql/core/langver
    contrib/ydb/library/yql/core/layers

    library/cpp/resource
    library/cpp/getopt
    library/cpp/yson/node
    library/cpp/yson
    library/cpp/logger

    contrib/libs/protobuf
)

YQL_LAST_ABI_VERSION()

END()
