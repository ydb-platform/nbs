LIBRARY()

SRCS(
    file_storage.cpp
)

PEERDIR(
    contrib/ydb/library/yql/utils/log
)

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()

