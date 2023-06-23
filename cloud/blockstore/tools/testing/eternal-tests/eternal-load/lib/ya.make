LIBRARY()

SRCS(
    config.cpp
# TODO: after DEVTOOLSSUPPORT-29629 remove config.sc.h and return next line
#    config.sc
    test_executor.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics

    library/cpp/aio
    library/cpp/config
    library/cpp/deprecated/atomic
    library/cpp/digest/crc32c
)

END()
