PROGRAM(fstest)

ALLOCATOR(SYSTEM)

NO_RUNTIME()
NO_SANITIZE()

SRCS(
    fstest.c
)

END()

RECURSE(
    bin
    tests
)
