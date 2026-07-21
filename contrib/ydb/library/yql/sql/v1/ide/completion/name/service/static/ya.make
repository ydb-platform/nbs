LIBRARY()

SRCS(
    name_index.cpp
    name_service.cpp
    name_set_json.cpp
    name_set.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/union
    contrib/ydb/library/yql/sql/v1/ide/completion/text
)

RESOURCE(
    contrib/ydb/library/yql/data/language/pragmas_opensource.json pragmas_opensource.json
    contrib/ydb/library/yql/data/language/types.json types.json
    contrib/ydb/library/yql/data/language/sql_functions.json sql_functions.json
    contrib/ydb/library/yql/data/language/udfs_basic.json udfs_basic.json
    contrib/ydb/library/yql/data/language/statements_opensource.json statements_opensource.json
    contrib/ydb/library/yql/data/language/rules_corr_basic.json rules_corr_basic.json
)

END()
