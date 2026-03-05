import xml.etree.ElementTree as ET

from scripts.tests import junit_utils as ju


def test_add_junit_property_replaces_existing_value():
    case = ET.Element("testcase")
    ju.add_junit_property(case, "x", "1")
    ju.add_junit_property(case, "x", "2")

    props = case.find("properties").findall("property")
    assert len(props) == 1
    assert props[0].get("value") == "2"


def test_iter_xml_files_reads_testsuite_and_testsuites(tmp_path):
    suite_only = tmp_path / "suite.xml"
    root1 = ET.Element("testsuite")
    ET.SubElement(root1, "testcase", {"classname": "a", "name": "b"})
    ET.ElementTree(root1).write(suite_only)

    suites = tmp_path / "suites.xml"
    root2 = ET.Element("testsuites")
    suite = ET.SubElement(root2, "testsuite")
    ET.SubElement(suite, "testcase", {"classname": "c", "name": "d"})
    ET.ElementTree(root2).write(suites)

    results = list(ju.iter_xml_files(str(tmp_path)))
    names = {(case.get("classname"), case.get("name")) for _fn, _suite, case in results}
    assert names == {("a", "b"), ("c", "d")}
