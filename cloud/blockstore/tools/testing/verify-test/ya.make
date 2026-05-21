PROGRAM()

GENERATE_ENUM_SERIALIZATION(test_executor.h)

SRCS(
    app.cpp
    main.cpp
    options.cpp
    test.cpp
    test_executor.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/threading/future
    library/cpp/deprecated/atomic
)

SPLIT_DWARF()

END()
