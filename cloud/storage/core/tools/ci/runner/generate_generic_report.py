#!/usr/bin/env python3

import report_common

from lxml import etree

import json
import os
import sys


def sort_by_name(parent):
    parent[:] = sorted(parent, key=lambda x: x.get("name"))


def on_test_case_result(
    suite_type,
    suite_date_str,
    link_prefix,
    test_case_name,
    test_case_result,
    output_element,
):
    params = etree.SubElement(output_element, "params")
    params.text = json.dumps(test_case_result, indent=4)


test_kind = sys.argv[1]
xsl_filename = sys.argv[2]

output_path = os.path.join(report_common.ROOT_DIR, "results", test_kind)

xslt = etree.parse(xsl_filename)

xml_output_path = output_path + ".xml"
html_output_path = output_path + ".html"

new_report = report_common.build_report(
    test_kind,
    on_test_case_result,
    output_path)

report_common.generate_report_files(
    new_report,
    xslt,
    xml_output_path,
    html_output_path)

for suite in new_report:
    suite_report = etree.Element("report")
    suite_report.append(suite)
    suite_path = os.path.join(output_path, suite.get("name"))
    report_common.generate_report_files(
        suite_report,
        xslt,
        suite_path + ".xml",
        suite_path + ".html")
