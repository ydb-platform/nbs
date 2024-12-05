LIBRARY()

SRCS(
    tablet_monitoring.cpp
)

PEERDIR(
    cloud/storage/core/libs/xsl_render

    library/cpp/monlib/service/pages

    contrib/ydb/core/base
)

END()
