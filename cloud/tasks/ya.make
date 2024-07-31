GO_LIBRARY()

SRCS(
    blank_task.go
    clear_ended_tasks_task.go
    collect_lister_metrics_task.go
    controller.go
    execution_context.go
    lister.go
    registry.go
    runner.go
    runner_metrics.go
    scheduler.go
    scheduler_impl.go
    storage_folder.go
    task.go
)

GO_TEST_SRCS(
    runner_test.go
    scheduler_test.go
    task_test.go
)

END()

RECURSE(
    common
    config
    errors
    headers
    logging
    operation
    persistence
    storage
    tracing
)

RECURSE_FOR_TESTS(
    acceptance_tests
    metrics
    mocks
    tasks_tests
    test
    tests
)
