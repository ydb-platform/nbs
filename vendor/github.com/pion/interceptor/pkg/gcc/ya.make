GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    adaptive_threshold.go
    arrival_group.go
    arrival_group_accumulator.go
    delay_based_bwe.go
    gcc.go
    kalman.go
    leaky_bucket_pacer.go
    loss_based_bwe.go
    noop_pacer.go
    overuse_detector.go
    rate_calculator.go
    rate_controller.go
    send_side_bwe.go
    slope_estimator.go
    state.go
    usage.go
)

GO_TEST_SRCS(
    adaptive_threshold_test.go
    arrival_group_accumulator_test.go
    arrival_group_test.go
    gcc_test.go
    kalman_test.go
    overuse_detector_test.go
    rate_calculator_test.go
    rate_controller_test.go
    send_side_bwe_test.go
    slope_estimator_test.go
)

END()

RECURSE(
    gotest
)
