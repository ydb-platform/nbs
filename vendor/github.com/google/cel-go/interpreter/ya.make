GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    activation.go
    attribute_patterns.go
    attributes.go
    decorators.go
    dispatcher.go
    evalstate.go
    interpretable.go
    interpreter.go
    optimizations.go
    planner.go
    prune.go
    runtimecost.go
)

GO_TEST_SRCS(
    activation_test.go
    attribute_patterns_test.go
    attributes_test.go
    interpreter_test.go
    prune_test.go
    runtimecost_test.go
)

END()

RECURSE(
    functions
    gotest
)
