LIBRARY()

SRCS(
    client.cpp
    config.cpp
    durable.cpp
    probes.cpp
    session_introspection.cpp
    session.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/api/grpc
    cloud/filestore/public/api/protos

    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/xsl_render

    library/cpp/lwtrace

    contrib/libs/grpc
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(ut)
