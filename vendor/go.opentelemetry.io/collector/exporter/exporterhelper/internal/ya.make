GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bounded_memory_queue.go
    mock_storage.go
    persistent_queue.go
    persistent_storage.go
    persistent_storage_batch.go
    producer_consumer_queue.go
    request.go
)

GO_TEST_SRCS(
    bounded_memory_queue_test.go
    persistent_queue_test.go
    persistent_storage_batch_test.go
    persistent_storage_test.go
)

END()

RECURSE(
    gotest
)
