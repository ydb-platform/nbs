import xml.etree.ElementTree as ET

from scripts.tests import split_junit as sj


def test_split_xml_creates_one_file_per_suite(tmp_path, build_multi_suite_xml):
    report = tmp_path / "junit.xml"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    build_multi_suite_xml(report, ("suite-a", "suite-b"))

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
