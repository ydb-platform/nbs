GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    event_broadcast.go
    last_usage_guard.go
    last_usage_guard_start.go
    map.go
    mutex.go
    once.go
    pool.go
    set.go
    soft_semaphore.go
    unbounded_chan.go
    value.go
)

GO_TEST_SRCS(
    event_broadcast_test.go
    last_usage_guard_test.go
    map_test.go
    mutex_test.go
    once_test.go
    pool_test.go
    set_test.go
    soft_semaphore_test.go
    unbounded_chan_test.go
    value_test.go
)

END()

RECURSE(
    gotest
)
