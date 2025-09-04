PROGRAM(rescompressor)

PEERDIR(
    library/cpp/resource
)

SRCDIR(
    tools/rescompressor
)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

END()
