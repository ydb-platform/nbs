GO_LIBRARY()

LICENSE(
    BSD-3-Clause AND
    MIT
)

SRCS(
    fields.go
    yaml.go
)

GO_TEST_SRCS(yaml_test.go)

END()

RECURSE(gotest)
