GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    bindings.go
    encoders.go
    formatting.go
    guards.go
    lists.go
    math.go
    native.go
    protos.go
    sets.go
    strings.go
)

GO_TEST_SRCS(
    bindings_test.go
    encoders_test.go
    lists_test.go
    math_test.go
    native_test.go
    protos_test.go
    sets_test.go
    strings_test.go
)

END()

RECURSE(
    gotest
)
