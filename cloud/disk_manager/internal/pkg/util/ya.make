GO_LIBRARY()

SRCS(
    json.go
    mlock.go
    proto.go
)

GO_TEST_SRCS(mlock_test.go)

END()

RECURSE_FOR_TESTS(tests)
