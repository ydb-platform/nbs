LIBRARY()

SRCS(
    tablet_monitoring.cpp
)

PEERDIR(
    library/cpp/monlib/service/pages
    library/cpp/xml/document

    contrib/ydb/core/base
)

END()
