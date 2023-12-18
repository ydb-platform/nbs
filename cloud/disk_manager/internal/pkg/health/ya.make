GO_LIBRARY()

SRCS(
    boot_disk.go
    flap_suppressor.go
    health.go
    nbs.go
    ydb.go
    s3.go
)

GO_TEST_SRCS(
    flap_suppressor_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
