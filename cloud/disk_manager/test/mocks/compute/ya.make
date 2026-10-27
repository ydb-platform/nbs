GO_PROGRAM(compute-mock)

SRCS(
    main.go
)

GO_TEST_SRCS(
    main_test.go
)

END()

RECURSE(
    config
)
