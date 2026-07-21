LIBRARY()

SRCS(
    dummy.cpp
    frequency.cpp
    ranking.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
)

RESOURCE(
    contrib/ydb/library/yql/data/language/rules_corr_basic.json rules_corr_basic.json
)

END()

RECURSE_FOR_TESTS(
    ut
)
