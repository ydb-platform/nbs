PROGRAM(load)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
)

SPLIT_DWARF()

END()

RECURSE_FOR_TESTS(test)
