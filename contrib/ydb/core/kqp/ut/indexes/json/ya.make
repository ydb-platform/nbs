UNITTEST_FOR(contrib/ydb/core/kqp)

REQUIREMENTS(cpu:2)

FORK_SUBTESTS()
SPLIT_FACTOR(200)
SIZE(MEDIUM)

SRCS(
    kqp_indexes_json_auto_select_ut.cpp
    GLOBAL kqp_indexes_json_corpus_je_ut.cpp
    GLOBAL kqp_indexes_json_corpus_jejv_ut.cpp
    GLOBAL kqp_indexes_json_corpus_jv_ut.cpp
    kqp_indexes_json_tokens_ut.cpp
    kqp_indexes_json_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/ut/indexes/common
    contrib/ydb/core/kqp/ut/indexes/json/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/library/json_index
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    common
)
