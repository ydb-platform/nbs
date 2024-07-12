GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    expand_json.go
    flatten_json.go
    normalize_json.go
    suppress_json_diff.go
)

GO_TEST_SRCS(
    expand_json_test.go
    flatten_json_test.go
    normalize_json_test.go
    suppress_json_diff_test.go
)

END()

RECURSE(
    gotest
)
