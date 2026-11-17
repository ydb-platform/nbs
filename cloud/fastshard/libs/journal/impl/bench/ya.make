PROGRAM(journalled-device-bench)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/fastshard/libs/journal/iface
    cloud/fastshard/libs/journal/impl

    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/json/writer
    library/cpp/string_utils/parse_size
    library/cpp/threading/future
)

END()
