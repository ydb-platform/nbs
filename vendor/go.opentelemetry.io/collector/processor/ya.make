GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    processor.go
)

GO_TEST_SRCS(processor_test.go)

END()

RECURSE(
    gotest
    processorhelper
    processortest
)
