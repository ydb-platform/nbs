GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    checked.go
    enum.go
    equal.go
    file.go
    pb.go
    type.go
)

GO_TEST_SRCS(
    equal_test.go
    file_test.go
    pb_test.go
    type_test.go
)

END()

RECURSE(
    gotest
)
