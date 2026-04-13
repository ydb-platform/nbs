GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    cityhash.go
    helpers.go
)

GO_TEST_SRCS(
    cityhash_test.go
    cityhash_testdata_test.go
)

END()

RECURSE(gotest)
