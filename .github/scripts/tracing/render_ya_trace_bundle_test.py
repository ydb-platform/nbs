import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).with_name("render_ya_trace_bundle.sh")
REPOSITORY_ROOT = Path(__file__).parents[3]


def _trace(ya_out: Path) -> None:
    path = ya_out / "suite" / "test-results" / "ut" / "ytest.report.trace"
    path.parent.mkdir(parents=True)
    path.write_text("{}\n")


def _command(tmp_path: Path) -> tuple[list[object], Path, Path, Path]:
    ya_out = tmp_path / "out"
    report = tmp_path / "report"
    summary = tmp_path / "summary.md"
    output = tmp_path / "github-output"
    _trace(ya_out)
    return (
        [
            "bash",
            SCRIPT,
            "--report-dir",
            report,
            "--ya-out",
            ya_out,
            "--html-url",
            "https://example.test/trace.html",
            "--otlp-url",
            "https://example.test/trace.otlp.jsonl.gz",
            "--inputs-url",
            "https://example.test/trace-inputs.tar.gz",
            "--summary",
            summary,
            "--github-output",
            output,
            "--",
            "--attempt-start-ns",
            "1",
            "--attempt-end-ns",
            "2",
            "--exit-code",
            "0",
        ],
        report,
        summary,
        output,
    )


def test_bundle_command_renders_packs_and_writes_links(tmp_path: Path) -> None:
    command, report, summary, output = _command(tmp_path)
    environment = {**os.environ, "PYTHONPATH": str(REPOSITORY_ROOT / ".github")}

    subprocess.run(command, cwd=REPOSITORY_ROOT, env=environment, check=True)

    assert (report / "trace.html").is_file()
    assert (report / "trace.otlp.jsonl.gz").is_file()
    assert (report / "trace-inputs.tar.gz").is_file()
    assert "[Execution trace]" in summary.read_text()
    assert output.read_text().splitlines() == [
        "html_url=https://example.test/trace.html",
        "otlp_url=https://example.test/trace.otlp.jsonl.gz",
        "inputs_url=https://example.test/trace-inputs.tar.gz",
    ]


def test_renderer_failure_keeps_raw_inputs_and_returns_success(tmp_path: Path) -> None:
    command, report, summary, output = _command(tmp_path)
    binaries = tmp_path / "bin"
    binaries.mkdir()
    python = binaries / "python3"
    python.write_text("#!/usr/bin/env bash\nexit 17\n")
    python.chmod(0o755)
    environment = {**os.environ, "PATH": f"{binaries}:{os.environ['PATH']}"}

    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=True,
        text=True,
        capture_output=True,
    )

    assert "::warning title=Trace report::Unable to generate" in completed.stdout
    assert not (report / "trace.html").exists()
    assert (report / "trace-inputs.tar.gz").is_file()
    assert "[Raw trace inputs]" in summary.read_text()
    assert output.read_text().splitlines() == [
        "inputs_url=https://example.test/trace-inputs.tar.gz"
    ]
