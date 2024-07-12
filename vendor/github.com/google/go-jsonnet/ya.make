GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    builtins.go
    doc.go
    error_formatter.go
    imports.go
    interpreter.go
    runtime_error.go
    thunks.go
    util.go
    value.go
    vm.go
    yaml.go
)

GO_TEST_SRCS(
    builtins_benchmark_test.go
    interpreter_test.go
    jsonnet_test.go
    main_test.go
)

END()

RECURSE(
    ast
    astgen
    cmd
    formatter
    gotest
    internal
    linter
    toolutils
)
