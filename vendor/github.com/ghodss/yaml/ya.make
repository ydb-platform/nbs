GO_LIBRARY()

LICENSE(
    BSD-3-Clause AND
    MIT
)

VERSION(v1.0.0)

SRCS(
    fields.go
    yaml.go
)

GO_TEST_SRCS(yaml_test.go)

END()

RECURSE(
    gotest
)
