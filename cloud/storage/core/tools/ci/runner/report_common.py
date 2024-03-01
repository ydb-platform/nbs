#!/usr/bin/env python3

from lxml import etree

import json
import os
import sys


ROOT_DIR = "/var/www/build"


def sort_by_name(parent):
    parent[:] = sorted(parent, key=lambda x: x.get("name"))


def build_report(test_kind, on_test_case_result, results_dir):
    report = etree.Element("report")
    report.set("name", test_kind)

    for root, dir, files in os.walk(results_dir):
        found_json = False
        for f in files:
            if f.endswith(".json"):
                found_json = True
                break

        if not found_json:
            continue

        suite = etree.SubElement(report, "suite")
        suite_name = root.split("/%s/" % test_kind)[1]
        suite_type, suite_date_str = os.path.split(suite_name)
        suite.set("name", suite_name)
        suite.set("type", suite_type)
        suite.set("date", suite_date_str)
        link_prefix = "/ci/results/" + test_kind
        suite.set("link", os.path.join(link_prefix, suite_name + ".html"))
        suite.set("stdout", os.path.join(link_prefix, suite_name, "stdout.txt"))
        suite.set("stderr", os.path.join(link_prefix, suite_name, "stderr.txt"))

        for f in files:
            if f.endswith(".json"):
                run_path = os.path.join(root, f)
                print("processing run %s" % run_path, file=sys.stderr)
                with open(run_path) as data:
                    try:
                        test_case_results = json.load(data)
                    except Exception as e:
                        test_case_results = [{
                            "error_message": "result parsing error: %s" % e
                        }]

                    i = 0
                    for test_case_result in test_case_results:
                        test_case = etree.SubElement(suite, "test-case")
                        test_case_name = os.path.splitext(f)[0]
                        test_case.set("name", test_case_name)
                        error = test_case_result.get("error_message")
                        if error is not None:
                            test_case.set("error", error)
                        i += 1

                        if (color := test_case_result.get("highlight_color")) is not None:
                            test_case.set("highlight_color", color)

                        on_test_case_result(
                            suite_type,
                            suite_date_str,
                            link_prefix,
                            test_case_name,
                            test_case_result,
                            test_case)

        sort_by_name(suite)

    return report


def generate_report_files(report, xslt, xml_path, html_path):
    transform = etree.XSLT(xslt)
    tree = etree.ElementTree(report)
    tree.write(xml_path, encoding="utf8", pretty_print=True)
    report_html = transform(report)
    report_html.write(html_path, encoding="utf8", pretty_print=True)
