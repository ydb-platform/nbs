import json
import xml.etree.ElementTree as ET

from scripts.tests import transform_ya_junit as tyj


def _write_junit(path):
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite", {"name": "suite-name"})
    case = ET.SubElement(
        suite,
        "testcase",
        {"classname": "old.class", "name": "pkg.TestClass.test_method", "time": "1.0"},
    )
    failure = ET.SubElement(case, "failure")
    failure.text = "boom"
    ET.ElementTree(root).write(path)


def _write_trace(ya_out):
    trace_dir = ya_out / "suite-name" / "test-results" / "run-1"
    trace_dir.mkdir(parents=True)
    trace = trace_dir / "ytest.report.trace"
    trace.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "name": "subtest-finished",
                        "value": {
                            "class": "pkg::TestClass",
                            "subtest": "test_method",
                            "logs": {
                                "stdout": "$(BUILD_ROOT)/suite-name/stdout.log",
                                "logsdir": "$(BUILD_ROOT)/suite-name/logs",
                            },
                        },
                    }
                )
            ]
        )
    )


def _get_property_value(case, name):
    props = case.find("properties")
    if props is None:
        return None
    for prop in props.findall("property"):
        if prop.get("name") == name:
            return prop.get("value")
    return None


def test_transform_adds_links_and_copies_logs(tmp_path):
    ya_out = tmp_path / "ya-out"
    logs_file = ya_out / "suite-name" / "stdout.log"
    logs_file.parent.mkdir(parents=True)
    logs_file.write_text("hello-log")

    _write_trace(ya_out)

    report = tmp_path / "junit.xml"
    _write_junit(report)

    output = tmp_path / "out.xml"
    logs_out = tmp_path / "logs-out"

    with report.open("r") as fp:
        tyj.transform(
            fp,
            tyj.YaMuteCheck(),
            str(ya_out),
            False,
            "https://logs/",
            str(logs_out),
            0,
            str(output),
            "https://data",
        )

    tree = ET.parse(output)
    case = tree.getroot().find("./testsuite/testcase")

    assert case.get("classname") == "suite-name"
    assert (
        _get_property_value(case, "url:stdout") == "https://logs/suite-name/stdout.log"
    )
    assert (
        _get_property_value(case, "url:logs_directory")
        == "https://data/suite-name/logs"
    )
    assert (logs_out / "suite-name" / "stdout.log").exists()


def test_save_log_applies_truncation(tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src = src_dir / "a.log"
    src.write_bytes(b"1234567890")

    out_dir = tmp_path / "out"

    url = tyj.save_log(str(src_dir), str(src), str(out_dir), "https://logs/", 4)

    assert url == "https://logs/a.log"
    assert (out_dir / "a.log").read_bytes() == b"7890"
