GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    default_notepad.go
    log_counter.go
    notepad.go
)

GO_TEST_SRCS(
    default_notepad_test.go
    notepad_test.go
)

END()

RECURSE(
    gotest
)
