import pytest

from scripts.tracingv1.otlp import Ns, ResourceAttributes, span_status_message
from scripts.tracingv1.ya_trace_report import build_ya_trace


@pytest.mark.parametrize("operation", ["build", "tests"])
def test_root_failure_message_names_ya_operation(operation: str) -> None:
    trace = build_ya_trace(
        [],
        root_start_ns=Ns(1),
        root_end_ns=Ns(2),
        exit_code=0,
        result_code=17,
        resource=ResourceAttributes(),
        operation=operation,
    )

    assert span_status_message(next(trace.spans("ya"))) == (
        f"ya make {operation} result code 17"
    )
