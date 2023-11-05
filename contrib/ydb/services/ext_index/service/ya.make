LIBRARY()

SRCS(
    add_data.cpp
    add_index.cpp
    executor.cpp
    activation.cpp
    deleting.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/services/ext_index/metadata
    contrib/ydb/services/ext_index/common
    contrib/ydb/library/yql/minikql/jsonpath
    contrib/ydb/public/api/protos
)

END()
