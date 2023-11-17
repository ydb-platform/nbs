PROGRAM()

GENERATE_ENUM_SERIALIZATION(options.h)

SRCS(
    app.cpp
    initiator.cpp
    main.cpp
    options.cpp
    runnable.cpp
    target.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/service
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    library/cpp/getopt
    library/cpp/deprecated/atomic
)

END()
