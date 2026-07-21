LIBRARY()

SRCS(
    readproxy.cpp
)



PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/persqueue/pqtablet/batching
)

END()

RECURSE_FOR_TESTS(
)
