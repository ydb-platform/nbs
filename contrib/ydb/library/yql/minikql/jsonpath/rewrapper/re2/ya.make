LIBRARY()

PEERDIR(
    contrib/libs/re2
    contrib/ydb/library/yql/minikql/jsonpath/rewrapper
)

SRCS(
    GLOBAL re2.cpp
)

END()

