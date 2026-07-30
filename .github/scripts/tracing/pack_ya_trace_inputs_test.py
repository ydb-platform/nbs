import subprocess
import tarfile
from pathlib import Path

PACK_SCRIPT = Path(__file__).with_name("pack_ya_trace_inputs.sh")


def _trace_path(ya_out: Path, suite: str) -> Path:
    return ya_out / suite / "test-results" / "unittest" / "ytest.report.trace"


def test_pack_uses_python_selected_trace_inputs(tmp_path: Path) -> None:
    report_dir = tmp_path / "summary"
    ya_out = tmp_path / "out"
    selected = _trace_path(ya_out, "selected")
    ignored = _trace_path(ya_out, "ignored")
    for path in (selected, ignored):
        path.parent.mkdir(parents=True)
        path.write_text(path.parent.parent.parent.name)
    report_dir.mkdir()
    (report_dir / "trace-inputs.files").write_bytes(
        b"./selected/test-results/unittest/ytest.report.trace\0"
    )
    (report_dir / "trace-inputs.manifest.json").write_text("{}\n")
    evlog = tmp_path / "ya_evlog.jsonl"
    evlog.write_text("{}\n")

    subprocess.run(
        ["bash", PACK_SCRIPT, report_dir, ya_out, evlog],
        check=True,
    )

    with tarfile.open(report_dir / "trace-inputs.tar.gz") as archive:
        assert set(archive.getnames()) == {
            "trace-inputs.manifest.json",
            "ya_evlog.jsonl",
            "ya-out/selected/test-results/unittest/ytest.report.trace",
        }


def test_pack_finds_trace_inputs_when_report_generation_failed(
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "summary"
    ya_out = tmp_path / "out"
    trace = _trace_path(ya_out, "suite")
    trace.parent.mkdir(parents=True)
    trace.write_text("{}\n")
    (ya_out / "not-a-trace").write_text("ignored")
    evlog = tmp_path / "ya_evlog.jsonl"
    evlog.write_text("{}\n")

    subprocess.run(
        ["bash", PACK_SCRIPT, report_dir, ya_out, evlog],
        check=True,
    )

    with tarfile.open(report_dir / "trace-inputs.tar.gz") as archive:
        assert set(archive.getnames()) == {
            "ya_evlog.jsonl",
            "ya-out/suite/test-results/unittest/ytest.report.trace",
        }
