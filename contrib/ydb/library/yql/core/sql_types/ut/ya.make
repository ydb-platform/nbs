UNITTEST_FOR(contrib/ydb/library/yql/core/sql_types)

SRCS(
    match_recognize_ut.cpp
    normalize_name_ut.cpp
    window_number_and_direction_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/sql_types
)

SIZE(SMALL)

END()
