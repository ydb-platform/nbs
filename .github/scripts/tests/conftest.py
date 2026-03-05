import xml.etree.ElementTree as ET

import pytest


@pytest.fixture
def mk_testcase():
    def _mk(
        *,
        classname='suite.case',
        name='test',
        time='1.0',
        failure=None,
        error=None,
        skipped=False,
        props=None,
    ):
        testcase = ET.Element(
            'testcase',
            {'classname': classname, 'name': name, 'time': time},
        )

        if failure is not None:
            node = ET.SubElement(testcase, 'failure')
            node.text = failure

        if error is not None:
            node = ET.SubElement(testcase, 'error')
            node.text = error

        if skipped:
            ET.SubElement(testcase, 'skipped')

        if props:
            props_node = ET.SubElement(testcase, 'properties')
            for key, value in props.items():
                ET.SubElement(props_node, 'property', {'name': key, 'value': value})

        return testcase

    return _mk


@pytest.fixture
def write_junit_xml():
    def _write(path, *cases, suite_name='test-suite'):
        suite = ET.Element('testsuite', {'name': suite_name})
        for case in cases:
            suite.append(case)

        root = ET.Element('testsuites')
        root.append(suite)
        ET.ElementTree(root).write(path)

    return _write


@pytest.fixture
def build_multi_suite_xml():
    def _build(path, suite_names):
        root = ET.Element('testsuites')
        for suite_name in suite_names:
            suite = ET.SubElement(root, 'testsuite', {'name': suite_name})
            ET.SubElement(suite, 'testcase', {'classname': suite_name, 'name': 't'})
        ET.ElementTree(root).write(path)

    return _build
