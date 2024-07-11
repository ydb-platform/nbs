#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import sys


def transform_xml(results_filename, suite_name, suite_date, test_name, muted):
    root = ET.fromstring(open(results_filename, "r").read())

    suite_attributes = {
        'name': f'{suite_name}/{suite_date}',
        'type': suite_name,
        'date': suite_date,
        'link': f'/ci/results/{test_name}/{suite_name}/{suite_date}/junit_report.xml',
    }
    suite_element = ET.Element('suite', attrib=suite_attributes)

    for testcase in root.findall('.//testcase'):
        name = testcase.get('name')
        error = testcase.find('failure')
        skipped = testcase.find('skipped')

        test_case_attributes = {'name': name}
        if name in muted:
            test_case_attributes['skipped'] = 'muted test'
        elif error is not None:
            test_case_attributes['error'] = error.get('message')
        elif skipped is not None:
            test_case_attributes['skipped'] = skipped.get('message')

        test_case_element = ET.SubElement(suite_element, 'test-case', attrib=test_case_attributes)
        ET.SubElement(test_case_element, 'test-case-report')

    return suite_element


def append_to_report(file_path, my_xml):
    tree = ET.parse(file_path)
    report_element = tree.getroot()
    report_element.append(my_xml)
    tree.write(file_path)


if __name__ == "__main__":
    original_xml = sys.argv[1]

    suite_name = sys.argv[2]
    suite_date = sys.argv[3]

    index_file = sys.argv[4]
    test_name = sys.argv[5]

    new_xml = transform_xml(original_xml, suite_name, suite_date, test_name, sys.argv[6:])

    append_to_report(index_file, new_xml)
