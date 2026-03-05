import xml.etree.ElementTree as ET

from scripts.tests import split_junit as sj


def _build_multi_suite(path):
    root = ET.Element("testsuites")
    for suite_name in ("suite-a", "suite-b"):
        suite = ET.SubElement(root, "testsuite", {"name": suite_name})
        ET.SubElement(suite, "testcase", {"classname": suite_name, "name": "t"})
    ET.ElementTree(root).write(path)


def test_split_xml_creates_one_file_per_suite(tmp_path):
    report = tmp_path / "junit.xml"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _build_multi_suite(report)

    with report.open("r") as fp:
        sj.split_xml(fp, str(out_dir))

    part0 = out_dir / "part_0.xml"
    part1 = out_dir / "part_1.xml"

    assert part0.exists()
    assert part1.exists()

    tree0 = ET.parse(part0)
    tree1 = ET.parse(part1)

    assert tree0.getroot().find("testsuite").get("name") == "suite-a"
    assert tree1.getroot().find("testsuite").get("name") == "suite-b"
