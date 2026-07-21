LIBRARY()

PEERDIR(
    library/cpp/regex/hyperscan
    contrib/ydb/library/yql/minikql/jsonpath/rewrapper
)

SRCS(
    GLOBAL hyperscan.cpp
)

END()

