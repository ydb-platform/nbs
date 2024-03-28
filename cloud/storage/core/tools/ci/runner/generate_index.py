#!/usr/bin/env python3
from pathlib import Path

from lxml import etree

import os
import sys


__ROOT = "/var/www/build"


def suite_sorting_key(suite):
    return suite.get("name")


def sort_suites(parent):
    parent[:] = sorted(parent, key=suite_sorting_key)


def generate_github_tests_section(index, tests_xml_index_path):
    tree = etree.ElementTree()
    test_report_infos = tree.parse(tests_xml_index_path)

    tests = etree.SubElement(index, "tests")
    for t in test_report_infos[:3]:
        tests.append(t)


def generate_teamcity_nbs_internal_tests(index: etree.Element, teamcity_domain_file_path: Path):
    test_cases = {
        "acceptance": {
            "small": "disk_manager_acceptance_small",
            "medium": "NBS_Tests_DiskManager_Acceptance_NbNbsStableLab_Acc",
            "enormous": "NBS_Tests_DiskManager_Acceptance_NbNbsStableLab_Enormous",
        },
        "eternal": {
            "8gib": "eternal_8gib",
            "256gib": "NBS_Tests_DiskManager_Eternal_NbNbsStableLab_Eternal256gib",
            "8tib": "NBS_Tests_DiskManager_Eternal_NbNbsStableLab_Eternal8tib",
        },
        "sync": {
            "2gib": "NBS_Tests_DiskManager_Sync_Sync2gib",
        }
    }
    tree = etree.SubElement(index, 'nb-nbs-stable-lab-teamcity-tests')
    tree.set("teamcity-domain", teamcity_domain_file_path.read_text())
    for test_kind, value in test_cases.items():
        sub_element = etree.SubElement(tree, test_kind)
        for test_case_name, build_configuration_name in value.items():
            test_case_element = etree.SubElement(sub_element, "teamcity_testcase")
            test_case_element.set("test-case-name", test_case_name)
            test_case_element.set("build-configuration-name", build_configuration_name)


def generate_generic_tests_section(index, section_name, results_xml_path):
    tree = etree.ElementTree()
    full_path = os.path.join(results_xml_path, section_name + ".xml")
    if not os.path.exists(full_path):
        return

    suites = tree.parse(full_path)
    type2suites = {}
    for suite in suites:
        t = suite.get("type")
        if t not in type2suites:
            type2suites[t] = []
        type2suites[t].append(suite)

    results = etree.SubElement(index, section_name)

    for t, suites in type2suites.items():
        sort_suites(suites)
        for suite in reversed(suites[-3:]):
            suite_element = etree.SubElement(results, "suite-status")
            suite_element.set("name", suite.get("name"))
            suite_element.set("type", suite.get("type"))
            suite_element.set("link", suite.get("link"))
            ok = 0
            err = 0
            for test_case in suite:
                test_case_element = etree.SubElement(suite_element, "test-case-status")
                test_case_element.set("name", test_case.get("name"))
                error = test_case.get("error")
                if error is not None:
                    err += 1
                    status_color = "red"
                    status_message = error
                else:
                    ok += 1
                    status_color = "green"
                    status_message = "ok"

                test_case_element.set("status-color", status_color)
                test_case_element.set("status-message", status_message)
            suite_element.set("ok", str(ok))
            status_color = "green"
            if err != 0:
                suite_element.set("err", str(err))
                status_color = "red"
            suite_element.set("status-color", status_color)
            suite_element.set("status-message", "ok: {ok}, err: {err}".format(ok=ok, err=err))


def generate_nbs_section(index, results_xml_path):
    for tests_type in ["fio", "corruption", "check_emptiness"]:
        generate_generic_tests_section(index, tests_type, results_xml_path)


def generate_nfs_section(index, results_xml_path):
    for tests_type in ["nfs_fio", "nfs_corruption", "xfs", "coreutils"]:
        generate_generic_tests_section(index, tests_type, results_xml_path)


def generate_dm_section(index, results_xml_path):
    for tests_type in [
        "disk_manager_acceptance", "disk_manager_eternal", "disk_manager_sync"
    ]:
        generate_generic_tests_section(index, tests_type, results_xml_path)

def generate_degradation_section(index, results_xml_path):
    for tests_type in ["degradation_tests"]:
        generate_generic_tests_section(index, tests_type, results_xml_path)

xsl_filename = sys.argv[1]

# input
tests_xml_index_path = os.path.join(__ROOT, "tests_index.xml")
results_xml_path = os.path.join(__ROOT, "results")

# output
index_path = os.path.join(__ROOT, "index")
xml_index_path = index_path + ".xml"
html_index_path = index_path + ".html"

index = etree.Element("index")

generate_github_tests_section(index, tests_xml_index_path)
generate_nbs_section(index, results_xml_path)
generate_nfs_section(index, results_xml_path)
generate_dm_section(index, results_xml_path)
generate_degradation_section(index, results_xml_path)
generate_teamcity_nbs_internal_tests(index, Path(__ROOT) / 'teamcity.url')
tree = etree.ElementTree(index)
tree.write(xml_index_path, encoding="utf-8", pretty_print=True)

index_xslt = etree.parse(xsl_filename)
transform = etree.XSLT(index_xslt)
index_html = transform(index)
print(transform.error_log)

index_html.write(html_index_path, encoding="utf8", pretty_print=True)
