#!/usr/bin/env python3

import report_common

from functools import partial
from lxml import etree
import matplotlib.pyplot as plt

import json
import os
import sys


def build_bars(
    svg,
    read,
    read_abs,
    read_label,
    write,
    write_abs,
    write_label,
    x,
    y,
):
    block_height = 100
    block_padding = 20
    cur_padding = 0

    rect = etree.SubElement(svg, "rect")
    rect.set("x", "%s%%" % x)
    rect.set("y", str(y))
    rect.set("width", "%s%%" % read)
    rect.set("height", str(block_height))
    rect.set("fill", "red")
    x += read

    if read_abs:
        cur_padding += block_padding

        text = etree.SubElement(svg, "text")
        text.set("x", "%s%%" % (x + 0.2))
        text.set("y", str(y + block_height + cur_padding))
        text.text = "%s %s" % (read_abs, read_label)

        line = etree.SubElement(svg, "line")
        line.set("x1", "%s%%" % x)
        line.set("x2", "%s%%" % x)
        line.set("y1", str(y))
        line.set("y2", str(y + block_height + cur_padding))
        line.set("stroke", "grey")

    rect = etree.SubElement(svg, "rect")
    rect.set("x", "%s%%" % x)
    rect.set("y", str(y))
    rect.set("width", "%s%%" % write)
    rect.set("height", str(block_height))
    rect.set("fill", "blue")
    x += write

    rect = etree.SubElement(svg, "rect")
    rect.set("x", "%s%%" % x)
    rect.set("y", str(y))
    rect.set("width", "%s%%" % (block_height - x))
    rect.set("height", str(block_height))
    rect.set("fill", "grey")

    if write_abs:
        cur_padding += block_padding

        text = etree.SubElement(svg, "text")
        text.set("x", "%s%%" % (x + 0.2))
        text.set("y", str(y + block_height + cur_padding))
        text.text = "%s %s" % (write_abs, write_label)

        line = etree.SubElement(svg, "line")
        line.set("x1", "%s%%" % x)
        line.set("x2", "%s%%" % x)
        line.set("y1", str(y))
        line.set("y2", str(y + block_height + cur_padding))
        line.set("stroke", "grey")

    y += block_height + cur_padding + 10

    return (0, y)


def perf_limits_by_test(test_case_result):
    disk_type = test_case_result["type"]
    if disk_type == "network-hdd" or disk_type == "network-hdd-nonreplicated":
        max_iops = 2000
        max_bw = 240
    elif disk_type == "network-ssd":
        max_iops = 40000
        max_bw = 450
    else:
        max_iops = 75000
        max_bw = 1024

    return (max_iops, max_bw)


def build_test_case_report(test_case_result):
    report = etree.Element("test-case-report")

    svg = etree.SubElement(report, "svg")

    max_iops, max_bw = perf_limits_by_test(test_case_result)

    result = test_case_result["result"]
    read_iops = result["read_iops"]
    write_iops = result["write_iops"]
    read_bw = result["read_bw"] / 1024
    write_bw = result["write_bw"] / 1024

    max_iops = max(max_iops, read_iops + write_iops)
    max_bw = max(max_bw, read_bw + write_bw)

    read_iops_width = int(100 * read_iops / max_iops)
    write_iops_width = int(100 * write_iops / max_iops)
    read_bw_width = int(100 * read_bw / max_bw)
    write_bw_width = int(100 * write_bw / max_bw)

    x, y = build_bars(
        svg,
        read_iops_width,
        int(read_iops),
        "RIOPS",
        write_iops_width,
        int(write_iops),
        "WIOPS",
        0,
        0,
    )
    x, y = build_bars(
        svg,
        read_bw_width,
        int(read_bw),
        "R MiB/s",
        write_bw_width,
        int(write_bw),
        "W MiB/s",
        x,
        y,
    )

    svg.set("height", str(y))
    svg.set("width", "100%")

    del test_case_result["result"]

    params = etree.SubElement(report, "params")
    params.text = json.dumps(test_case_result, indent=4)

    return report, read_iops, write_iops, read_bw, write_bw


def on_test_case_result_impl(
    suite_type,
    suite_date_str,
    link_prefix,
    test_case_name,
    test_case_result,
    output_element,
    by_all_time,
):
    if output_element.get("error"):
        return

    try:
        test_case_report, read_iops, write_iops, read_bw, write_bw = \
            build_test_case_report(test_case_result)
    except Exception as e:
        output_element.set("error", "exception: %s" % e)
        return

    output_element.append(test_case_report)

    by_all_time_key = os.path.join(suite_type, test_case_name)
    if by_all_time_key not in by_all_time:
        by_all_time[by_all_time_key] = TestHistory()
    by_all_time[by_all_time_key].points.append(TestHistoryPoint(
        suite_date_str,
        read_iops,
        write_iops,
        read_bw,
        write_bw,
    ))
    output_element.set(
        "history-link",
        os.path.join(link_prefix, relative_history_link(by_all_time_key))
    )
    output_element.set(
        "history-thumb",
        os.path.join(link_prefix, relative_history_thumb(by_all_time_key))
    )


class TestHistoryPoint:

    def __init__(self, date, read_iops, write_iops, read_bw, write_bw):
        self.date = date
        self.read_iops = read_iops
        self.write_iops = write_iops
        self.read_bw = read_bw
        self.write_bw = write_bw


class TestHistory:

    def __init__(self):
        self.points = []


def relative_history_link(test_tag):
    return test_tag + ".history.svg"


def relative_history_thumb(test_tag):
    return test_tag + ".history.thumb.svg"


def generate_history(fio_results_path, test_tag, history):
    dates = []
    read_iops = []
    write_iops = []
    read_bw = []
    write_bw = []

    history.points.sort(key=lambda x: x.date)

    for point in history.points:
        dates.append(point.date)
        read_iops.append(point.read_iops)
        write_iops.append(point.write_iops)
        read_bw.append(point.read_bw)
        write_bw.append(point.write_bw)

    width_per_14 = 16.
    full_fig_width = int(width_per_14 * max(1, len(dates) / 14.))
    thumb_width = width_per_14
    thumb_len = 14

    fig, axs = plt.subplots(2, 1, figsize=(full_fig_width, 9))
    axs[0].bar(dates, read_iops, color="red", label="read")
    axs[0].bar(dates, write_iops, bottom=read_iops, color="blue", label="write")
    axs[0].set_ylabel('IOPS')
    axs[0].legend()
    axs[1].bar(dates, read_bw, color="red", label="read")
    axs[1].bar(dates, write_bw, bottom=read_bw, color="blue", label="write")
    axs[1].set_ylabel('BW (MiB/s)')
    axs[1].legend()

    plt.savefig(os.path.join(fio_results_path, relative_history_link(test_tag)))
    plt.close(fig)

    fig, ax = plt.subplots(1, 1, figsize=(thumb_width, 3))
    ax.bar(
        dates[-thumb_len:],
        read_iops[-thumb_len:],
        color="red",
        label="read")
    ax.bar(
        dates[-thumb_len:],
        write_iops[-thumb_len:],
        bottom=read_iops[-thumb_len:],
        color="blue",
        label="write")
    ax.set_ylabel('IOPS')
    ax.legend()
    # ax.tick_params(axis='x', labelrotation=45)

    plt.savefig(os.path.join(fio_results_path, relative_history_thumb(test_tag)))
    plt.close(fig)


xsl_filename = sys.argv[1]
suite_kind = sys.argv[2]

xslt = etree.parse(xsl_filename)

fio_results_path = os.path.join(report_common.ROOT_DIR, "results", suite_kind)
xml_output_path = fio_results_path + ".xml"
html_output_path = fio_results_path + ".html"

by_all_time = {}

new_report = report_common.build_report(
    suite_kind,
    partial(on_test_case_result_impl, by_all_time=by_all_time),
    fio_results_path)

for test_tag, history in by_all_time.items():
    generate_history(fio_results_path, test_tag, history)

report_common.generate_report_files(
    new_report,
    xslt,
    xml_output_path,
    html_output_path)

for suite in new_report:
    suite_report = etree.Element("report")
    suite_report.append(suite)
    suite_path = os.path.join(fio_results_path, suite.get("name"))
    report_common.generate_report_files(
        suite_report,
        xslt,
        suite_path + ".xml",
        suite_path + ".html")
