from __future__ import annotations

import base64
import gzip
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import UndefinedError
from opentelemetry.proto_json.trace.v1.trace import TracesData

from scripts.tracing.ya_trace_report import (
    build_resource_attributes,
    build_ya_trace,
)
from scripts.tracing.trace_io import read_otlp_jsonl
from scripts.tracing.otlp import (
    ResourceAttributes,
    Span,
    Trace,
    decode_attributes,
    make_event,
    make_span as new_span,
    Ns,
)
from scripts.tracing.trace_report import (
    TRACE_HTML_TEMPLATE,
    TRACE_SCRIPT_TEMPLATE,
    TRACE_TEMPLATE_ENV,
    _trace_model,
    render_html,
    write_trace_bundle,
)
from scripts.tracing.yatrace import YaTraceCollection, load_ya_evlog


def make_span(**kwargs) -> Span:
    values = {
        "trace_id": bytes.fromhex("1" * 32),
        "span_id": bytes.fromhex("2" * 16),
        "name": "root",
        "start_ns": 1_000,
        "end_ns": 2_000,
    }
    values.update(kwargs)
    return new_span(**values)


def make_trace(
    *spans: Span,
    resource: ResourceAttributes | None = None,
    scope_name: str = "nbs.ci",
) -> Trace:
    trace = Trace()
    for span in spans:
        trace.add_span(
            span,
            resource=resource or ResourceAttributes(),
            scope_name=scope_name,
        )
    return trace


def attributes(span: Span) -> dict:
    return decode_attributes(span.attributes)


def test_otlp_round_trip_and_static_html_escaping(tmp_path: Path) -> None:
    trace = make_trace(
        make_span(
            attributes={
                "string": '<script>alert("attribute")</script>',
                "integer": 42,
                "boolean": True,
                "array": ["one", "two"],
            },
            events=[
                make_event(
                    name="<event>",
                    time_ns=1_500,
                    attributes={"detail": "<b>unsafe</b>"},
                )
            ],
            status_code=2,
            status_message="<failed>",
        ),
        resource=ResourceAttributes({"service.name": "test"}),
    )

    assert isinstance(trace.data, TracesData)
    encoded_span = trace.to_dict()["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert encoded_span["traceId"] == "1" * 32
    assert encoded_span["spanId"] == "2" * 16
    assert encoded_span["startTimeUnixNano"] == "1000"

    manifest = write_trace_bundle(
        tmp_path,
        trace,
        title="<trace>",
        metadata={
            "github.sha": "<sha>",
            "github.run.url": "https://github.example/run?name=<trace>",
        },
    )

    restored = read_otlp_jsonl([tmp_path / manifest["otlp_file"]])
    assert restored.to_dict() == trace.to_dict()
    assert isinstance(restored[0].start_time_unix_nano, Ns)
    assert isinstance(restored[0].events[0].time_unix_nano, Ns)

    report = (tmp_path / manifest["html_file"]).read_text()
    assert "<script>alert" not in report
    assert "&lt;sha&gt;" in report
    assert (
        'href="https://github.example/run?name=&lt;trace&gt;"'
        ">https://github.example/run?name=&lt;trace&gt;</a>"
    ) in report
    assert "<h1>&lt;trace&gt;</h1>" in report
    assert "default-src &#x27;none&#x27;" not in report
    payload = re.search(
        r'<script id="trace-data"[^>]*>([^<]+)</script>',
        report,
    )
    assert payload
    model = json.loads(gzip.decompress(base64.b64decode(payload.group(1))))
    assert model["s"][0][5]["string"] == '<script>alert("attribute")</script>'
    assert manifest["span_count"] == 1


def test_artifact_url_prefixes_are_html_only_and_validated(tmp_path: Path) -> None:
    trace = make_trace(
        make_span(
            status_code=2,
            attributes={
                "ya.test.log.stdout.path": "suite/output.log",
                "ya.test.logs_directory.path": "suite/testing_out_stuff",
            },
        )
    )
    log_prefix = "https://artifacts.example.test/logs/1"
    data_prefix = "https://artifacts.example.test/test_data/1/workspace/"

    manifest = write_trace_bundle(
        tmp_path,
        trace,
        title="trace with links",
        test_log_url_prefix=log_prefix,
        test_data_url_prefix=data_prefix,
    )

    payload = re.search(
        r'<script id="trace-data"[^>]*>([^<]+)</script>',
        (tmp_path / manifest["html_file"]).read_text(),
    )
    assert payload
    model = json.loads(gzip.decompress(base64.b64decode(payload.group(1))))
    assert model["u"] == {
        "testLog": f"{log_prefix}/",
        "testData": data_prefix,
    }
    with gzip.open(tmp_path / manifest["otlp_file"], "rt", encoding="utf-8") as f:
        otlp = f.read()
    assert log_prefix not in otlp
    assert data_prefix not in otlp
    assert "testLog" not in manifest
    assert "testData" not in manifest

    invalid_model = _trace_model(
        trace,
        test_log_url_prefix="javascript:alert(1)",
        test_data_url_prefix="https://artifacts.example.test/data?unsafe=1",
    )
    assert invalid_model["u"] == {}


def test_renderer_marks_error_spans_and_supports_empty_input() -> None:
    trace = make_trace(make_span(status_code=2))
    assert _trace_model(trace)["s"][0][7] == 2
    report = render_html(trace)
    assert 'id="trace-data"' in report
    assert "Rows per load" in report
    assert '<select id="row-load-size" disabled>' in report
    assert '<option value="200" selected>200</option>' in report
    assert '<option value="1000">1,000</option>' in report
    assert '<option value="5000">5,000</option>' in report
    assert (
        'id="filter" class="text-filter" type="search" aria-label="Filter spans or attributes"'
        in report
    )
    assert 'id="clear-filter" class="inline-clear" type="button"' in report
    assert (
        'aria-label="Clear text filter" title="Clear text filter" hidden disabled'
        in report
    )
    assert 'id="clear-minimum-duration" class="inline-clear" type="button"' in report
    assert (
        'aria-label="Clear minimum duration" title="Clear minimum duration" hidden disabled'
        in report
    )
    assert 'id="clear-filters" type="button" aria-label="Clear filters"' in report
    assert 'title="Clear all filters" disabled>Clear filters</button>' in report
    assert '<select id="test-phase"' in report
    assert '<option value="">Any matching span</option>' in report
    assert '<label class="timeline-mode-control" for="timeline-mode">Timeline' in report
    assert '<select id="timeline-mode"' in report
    assert '<option value="global">Global trace</option>' in report
    assert '<option value="local" selected>Local test intervals</option>' in report
    assert 'id="load-rows"' in report
    assert "Load next 200 rows" in report
    assert 'id="row-status"' in report
    assert 'role="status" aria-live="polite"' in report
    assert "All (may be slow)" not in report
    assert "No spans found" in render_html(Trace())


def test_trace_template_rejects_missing_context() -> None:
    template = TRACE_TEMPLATE_ENV.get_template(TRACE_HTML_TEMPLATE.name)
    with pytest.raises(UndefinedError):
        template.render()


def test_trace_script_is_safe_to_embed() -> None:
    scripts = (TRACE_SCRIPT_TEMPLATE.parent / "trace_report").glob("*.js")
    assert all("</script" not in script.read_text().lower() for script in scripts)


def test_resource_attributes_support_environment_and_workflow_run() -> None:
    environment = {
        "GITHUB_SERVER_URL": "https://github.example",
        "GITHUB_REPOSITORY": "example/repo",
        "GITHUB_WORKFLOW": "Build",
        "GITHUB_RUN_ID": "123",
        "GITHUB_SHA": "abc123",
    }
    current = ResourceAttributes.from_github(environment=environment)
    assert current["github.run.url"] == (
        "https://github.example/example/repo/actions/runs/123"
    )
    assert current["github.commit.url"] == (
        "https://github.example/example/repo/commit/abc123"
    )

    completed = ResourceAttributes.from_github(
        environment=environment,
        workflow_run={
            "id": 456,
            "name": "Nightly",
            "head_sha": "def456",
            "repository": {"full_name": "upstream/repo"},
            "pull_requests": [{"number": 42}],
        },
    )
    assert completed["github.run.id"] == 456
    assert completed["github.workflow"] == "Nightly"
    assert completed["github.pull_request.number"] == [42]
    assert completed["github.commit.url"] == (
        "https://github.example/upstream/repo/commit/def456"
    )


def test_ya_resource_attributes_extend_common_github_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    attributes = build_resource_attributes(
        SimpleNamespace(
            operation="tests",
            component="tasks",
            build_preset="relwithdebinfo",
            build_target="",
            test_target="cloud/tasks",
            test_type="unittest",
            test_size="small",
            retry=2,
        )
    )
    assert attributes["service.name"] == "nbs-ya-tests"
    assert attributes["github.run.url"].endswith("/example/repo/actions/runs/123")
    assert attributes["ci.test.target"] == "cloud/tasks"


def test_renderer_collapses_generated_ya_groups_by_default(tmp_path: Path) -> None:
    trace_path = (
        tmp_path
        / "out"
        / "cloud/tasks/storage/tests"
        / "test-results"
        / "unittest"
        / "ytest.report.trace"
    )
    trace_path.parent.mkdir(parents=True)
    trace_path.write_text(
        "".join(
            json.dumps(event) + "\n"
            for event in (
                {
                    "name": "subtest-finished",
                    "timestamp": 6,
                    "value": {
                        "class": "TestSuite",
                        "subtest": "test_case",
                        "status": "good",
                        "time": 1,
                    },
                },
                {
                    "name": "subtest-started",
                    "timestamp": 6.5,
                    "value": {
                        "class": "TestSuite",
                        "subtest": "unfinished",
                    },
                },
                {
                    "name": "chunk-event",
                    "timestamp": 7,
                    "value": {
                        "chunk_index": 0,
                        "nchunks": 1,
                        "metrics": {
                            "suite_start_timestamp": 4,
                            "suite_finish_timestamp": 7,
                            "suite_prepare_recipes_(seconds)": 0.25,
                        },
                    },
                },
            )
        )
    )
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.write_text(
        "".join(
            json.dumps(event) + "\n"
            for event in (
                {
                    "namespace": "stages",
                    "event": "stage-finished",
                    "value": {
                        "name": "dispatch_build",
                        "tag": "dispatch_build",
                        "time": [1, 8],
                    },
                },
                {
                    "namespace": "worker_threads",
                    "event": "node-finished",
                    "value": {
                        "name": "Run(uid$(BUILD_ROOT)/output.o)",
                        "tag": "CC",
                        "time": [2, 3],
                    },
                },
            )
        )
    )
    trace = build_ya_trace(
        YaTraceCollection.load(tmp_path / "out").traces,
        root_start_ns=Ns(0),
        root_end_ns=Ns.from_s(9),
        exit_code=0,
        resource=ResourceAttributes(),
        evlog=load_ya_evlog(evlog_path),
    )

    model = _trace_model(trace)
    encoded = {span[2]: span for span in model["s"]}
    indexes = {span[2]: index for index, span in enumerate(model["s"])}
    report = render_html(trace)
    chunk_name = "cloud/tasks/storage/tests [unittest chunk 1/1]"
    phase_name = next(trace.spans("ya.phase")).name
    assert encoded["build operations"][1] == indexes[phase_name]
    assert encoded["CC: output.o"][1] == indexes["build operations"]
    assert encoded["test operations"][1] == indexes[phase_name]
    assert encoded[chunk_name][1] == indexes["test operations"]
    assert encoded["TestSuite::test_case"][1] == indexes[chunk_name]
    assert encoded["TestSuite::unfinished"][1] == indexes[chunk_name]
    assert encoded["TestSuite::test_case"][5]["test.timing.inferred"] is True
    assert encoded["TestSuite::unfinished"][5]["test.incomplete"] is True
    assert (
        encoded[chunk_name][5]["ya.chunk.metric.suite_prepare_recipes_seconds"] == 0.25
    )
    assert {
        "ya.build",
        "ya.build.node",
        "ya.chunk",
        "ya.test",
        "ya.test.operations",
        "ya.test.stage",
    } <= set(model["c"])
    template = TRACE_HTML_TEMPLATE.read_text()
    scripts = (TRACE_SCRIPT_TEMPLATE.parent / "trace_report").glob("*.js")
    assert '{% include "trace_report.js" %}' in template
    assert all(script.read_text() in report for script in scripts)
    assert '{% include "trace_report.js" %}' not in report
    assert "{% include" not in report
    assert "CC: output.o" not in report
