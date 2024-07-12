GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    angle.go
    chordangle.go
    doc.go
    interval.go
)

GO_TEST_SRCS(
    angle_test.go
    chordangle_test.go
    interval_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
