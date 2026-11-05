PROGRAM(fstest)

ALLOCATOR(SYSTEM)

NO_RUNTIME()
NO_SANITIZE()

SRCS(
    fstest.c
)

PEERDIR(
    cloud/filestore/tools/testing/fs_posix_compliance/suite/libacl
)

END()

RECURSE(
    bin
    libacl
    tests
)
