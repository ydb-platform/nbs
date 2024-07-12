GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    normalize_obj.go
)

GO_TEST_SRCS(normalize_obj_test.go)

END()

RECURSE(
    gotest
)
