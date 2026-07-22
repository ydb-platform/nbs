PROGRAM(codec_bench)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/getopt
)

END()
