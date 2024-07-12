GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    sampleSequenceLocation.go
    samplebuilder.go
)

GO_TEST_SRCS(
    sampleSequenceLocation_test.go
    samplebuilder_test.go
)

END()

RECURSE(
    gotest
)
