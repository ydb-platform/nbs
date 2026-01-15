LIBRARY()

SRCS(
    https.cpp
    notify.cpp
    json_generator.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/notify/iface

    library/cpp/http/io
    library/cpp/threading/future

    contrib/libs/curl
)

END()

RECURSE_FOR_TESTS(ut)
