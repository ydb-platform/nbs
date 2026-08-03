from scripts.tracing.otlp import Trace
from scripts.tracing.trace_report import render_html


def test_filter_form_is_the_single_native_filter_state() -> None:
    report = render_html(Trace())

    assert '<form id="filters" class="filter-form">' in report
    for name in (
        "query",
        "failed",
        "top-tests",
        "phase",
        "minimum-duration",
        "test-size",
    ):
        assert f'name="{name}"' in report
    assert 'id="clear-filters" type="reset"' in report
