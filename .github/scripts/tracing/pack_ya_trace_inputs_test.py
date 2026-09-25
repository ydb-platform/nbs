import subprocess
import tarfile
from pathlib import Path

PACK_SCRIPT = Path(__file__).with_name("pack_ya_trace_inputs.sh")


def _trace_path(ya_out: Path, suite: str) -> Path:
    return ya_out / suite / "test-results" / "unittest" / "ytest.report.trace"


def test_pack_uses_python_selected_paths_without_auxiliary_manifest(
    tmp_path: Path,
) -> None:
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
    evlog = tmp_path / "ya_evlog.jsonl"
    evlog.write_text("{}\n")

    subprocess.run(["bash", PACK_SCRIPT, report_dir, ya_out, evlog], check=True)

    with tarfile.open(report_dir / "trace-inputs.tar.gz") as archive:
        assert set(archive.getnames()) == {
            "ya_evlog.jsonl",
            "ya-out/selected/test-results/unittest/ytest.report.trace",
        }
    assert not (report_dir / "trace-inputs.files").exists()


def test_pack_discovers_inputs_after_renderer_startup_failure(tmp_path: Path) -> None:
    report_dir = tmp_path / "summary"
    ya_out = tmp_path / "out"
    trace = _trace_path(ya_out, "suite")
    trace.parent.mkdir(parents=True)
    trace.write_text("{}\n")
    (ya_out / "not-a-trace").write_text("ignored")

    subprocess.run(["bash", PACK_SCRIPT, report_dir, ya_out], check=True)

    with tarfile.open(report_dir / "trace-inputs.tar.gz") as archive:
        assert archive.getnames() == [
            "ya-out/suite/test-results/unittest/ytest.report.trace"
        ]


def test_pack_treats_unusual_python_selected_paths_literally(tmp_path: Path) -> None:
    report_dir = tmp_path / "summary"
    ya_out = tmp_path / "out"
    trace = _trace_path(ya_out, "-suite\nwith-newline")
    trace.parent.mkdir(parents=True)
    trace.write_text("{}\n")
    report_dir.mkdir()
    relative = trace.relative_to(ya_out).as_posix()
    (report_dir / "trace-inputs.files").write_bytes(f"./{relative}".encode() + b"\0")

    subprocess.run(["bash", PACK_SCRIPT, report_dir, ya_out], check=True)

    with tarfile.open(report_dir / "trace-inputs.tar.gz") as archive:
        assert archive.getnames() == [f"ya-out/{relative}"]
