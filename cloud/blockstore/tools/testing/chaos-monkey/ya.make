GO_PROGRAM()

SRCS(
    analyze_state.go
    main.go
    nbs.go
)

GO_TEST_SRCS(
    monkey_test.go
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()

RECURSE_FOR_TESTS(tests)
