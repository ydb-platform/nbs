GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    fixedbig.go
    replaydetector.go
)

GO_TEST_SRCS(
    fixedbig_test.go
    replaydetector_test.go
)

END()

RECURSE(
    gotest
)
