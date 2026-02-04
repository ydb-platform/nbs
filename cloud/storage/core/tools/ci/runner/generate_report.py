#!/usr/bin/env python3

from lxml import etree

import os
import re
import sys
import time


__ROOT = "/var/www/build"


def suite_sorting_key(suite):
    return "%s::%s" % (suite.get("priority"), suite.get("name"))


def sort_suites(parent):
    parent[:] = sorted(parent, key=suite_sorting_key)


def report_info_sorting_key(report_info):
    return -int(report_info.get("ts"))


def sort_report_infos(parent):
    parent[:] = sorted(parent, key=report_info_sorting_key)


def build_report(report_name, logs_dir):
    report = etree.Element("report")
    report.set("name", report_name)

    done_pattern = re.compile("([0-9]+): \[DONE\] (.*)")
    result_pattern = re.compile("([0-9]+)% tests passed, ([0-9]+) tests failed out of ([0-9]+)")

    for root, dir, files in os.walk(logs_dir):
        if not len(files):
            continue

        f = "ya-make-junit-result.xml"
        if f not in files:
            continue

        tree = etree.parse(os.path.join(root, f))
        testsuites = tree.getroot()
        for testsuite in testsuites:
            suite_name = os.path.join(report_name, testsuite.attrib['name'])
            all = len(testsuite.findall(".//testcase"))
            err = int(testsuite.attrib['failures'])
            skip = int(testsuite.attrib['skipped'])
            ok = all - err - skip

            suites = report.findall(".//suite[@name='{name}']".format(name=suite_name))
            if suites:
                suite = suites[0]
            else:
                suite = etree.SubElement(report, "suite")
                suite.set("name", suite_name)

                artifact = etree.SubElement(suite, "artifact")
                artifact.set("name", "test_dir")
                artifact.set("link", "/ci/logs/%s" % suite_name)

                suite.set("ok", str(0))
                suite.set("err", str(0))
                suite.set("skip", str(0))

            ok += int(suite.attrib['ok'])
            err += int(suite.attrib['err'])
            skip += int(suite.attrib['skip'])

            status_color = "green"
            priority = "5"
            if err != 0 or all == 0 or ok < 0:
                status_color = "red"
                priority = "0"
                if err == 0:
                    err = 1

            suite.set("status-color", status_color)
            suite.set("status-message", "[DONE] ok: {ok}, err: {err}, skip: {skip}".format(ok=ok, err=err, skip=skip))
            suite.set("priority", priority)
            suite.set("ok", str(ok))
            suite.set("err", str(err))
            suite.set("skip", str(skip))

    sort_suites(report)

    return report


def build_report_info(report, html_output_path, junit_report_html_path):
    report_info = etree.Element("report-info")
    report_info.set("name", report.get("name"))
    report_info.set("ts", str(int(time.time())))
    report_info.set("link", "/ci" + html_output_path[len(__ROOT):])
    report_info.set("junit-link", "/ci" + junit_report_html_path[len(__ROOT):])
    ok = 0
    err = 0
    for suite in report:
        ok += int(suite.get("ok"))
        err += int(suite.get("err"))
    status_message = "ok: %s" % ok
    status_color = "green"
    if err > 0:
        status_message += ", err: %s" % err
        status_color = "red"
    report_info.set("status-message", status_message)
    report_info.set("status-color", status_color)
    return report_info


def main():
    logs_dir = sys.argv[1]
    xsl_filename = sys.argv[2]
    index_xsl_filename = sys.argv[3]
    new_report_name = os.path.basename(logs_dir)
    output_path = os.path.join(__ROOT, "results", "github", new_report_name)
    junit_report_html_path = os.path.join(
        __ROOT,
        "logs",
        new_report_name,
        "ya-make-junit-result.xml.html")
    index_path = os.path.join(__ROOT, "tests_index")

    xml_output_path = output_path + ".xml"
    html_output_path = output_path + ".html"
    xml_index_path = index_path + ".xml"
    html_index_path = index_path + ".html"

    new_report = build_report(new_report_name, logs_dir)

    tree = etree.ElementTree(new_report)
    tree.write(xml_output_path, encoding="utf8", pretty_print=True)

    xslt = etree.parse(xsl_filename)
    transform = etree.XSLT(xslt)
    new_report_html = transform(new_report)

    new_report_html.write(html_output_path, encoding="utf8", pretty_print=True)

    new_report_infos = {}
    if os.path.exists(xml_index_path):
        tree = etree.ElementTree()
        report_infos = tree.parse(xml_index_path)
        for report_info in report_infos:
            new_report_infos[report_info.get("name")] = report_info
    new_report_infos[new_report_name] = build_report_info(
        new_report,
        html_output_path,
        junit_report_html_path)

    new_report_info_list = [v for v in new_report_infos.values()]
    new_index = etree.Element("status")
    for ri in new_report_info_list:
        new_index.append(ri)
    sort_report_infos(new_index)

    tree = etree.ElementTree(new_index)
    tree.write(xml_index_path, encoding="utf-8", pretty_print=True)

    index_xslt = etree.parse(index_xsl_filename)
    transform = etree.XSLT(index_xslt)
    new_index_html = transform(new_index)

    new_index_html.write(html_index_path, encoding="utf8", pretty_print=True)


if __name__ == '__main__':
    main()
