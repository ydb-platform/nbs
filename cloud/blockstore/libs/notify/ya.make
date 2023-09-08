LIBRARY()

SRCS(
    config.cpp
    https.cpp
    notify.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics

    library/cpp/http/io
    library/cpp/threading/future

    contrib/libs/curl
)

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(ut)   # TODO(NBS-4409): add to opensource
ENDIF()
