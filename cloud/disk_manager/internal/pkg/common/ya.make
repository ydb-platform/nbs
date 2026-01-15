GO_LIBRARY()

SET(
    GO_VET_FLAGS
    -printf=false
)

SRCS(
    assert.go
    channel_with_cancellation.go
    channel_with_inflight_queue.go
    cond.go
    disk_kind.go
    errors.go
    inflight_queue.go
    progress_saver.go
    util.go
)

GO_TEST_SRCS(
    inflight_queue_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
