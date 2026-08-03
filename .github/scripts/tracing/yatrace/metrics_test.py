import scripts.tracing.yatrace.metrics as metrics


def test_metrics_helpers_have_public_descriptive_names() -> None:
    assert metrics.normalize_metric_name("C++ compiler") == "c_compiler"
    assert metrics.metric_attributes(
        {"Elapsed Time": 1.5},
        prefix="ya.metric",
    ) == {"ya.metric.elapsed_time": 1.5}
    assert metrics.finite_number(3) == 3
    assert metrics.finite_number(float("inf")) is None
    assert {"_metric_name", "_metric_attributes", "_number"}.isdisjoint(
        vars(metrics)
    )
