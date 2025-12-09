UNITTEST_FOR(contrib/ydb/library/backup)

SIZE(SMALL)

SRC(ut.cpp)

PEERDIR(
    library/cpp/string_utils/quote
    contrib/ydb/library/backup
)

END()
