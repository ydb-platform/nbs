PROGRAM(kms-example)

PEERDIR(
    cloud/blockstore/libs/kms/impl
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/string_utils/base64
)

SRCS(
    main.cpp
)

SPLIT_DWARF()

IF (SANITIZER_TYPE)
    NO_SPLIT_DWARF()
ENDIF()

END()
