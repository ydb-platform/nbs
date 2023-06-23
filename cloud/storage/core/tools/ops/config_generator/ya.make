GO_PROGRAM()

SRCS(
    main.go
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()

RECURSE_FOR_TESTS(
    gotest
)
