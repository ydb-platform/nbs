PROGRAM(filestore-client)

ALLOCATOR(TCMALLOC_TC)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/binaries_dependency.inc)

SPLIT_DWARF()

IF (SANITIZER_TYPE)
    NO_SPLIT_DWARF()
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    cloud/filestore/apps/client/lib
    cloud/storage/core/libs/iam/iface
    library/cpp/getopt
    library/cpp/terminate_handler
)

END()

RECURSE(
    lib
)
