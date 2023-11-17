#!/usr/bin/env python

from .trace import media_kind2str, extract_minimized_trace

from collections import defaultdict
from lxml import etree


REPORT_WIDTH = 2000
LABEL_WIDTH = 600
RECT_Y = 0
RECT_HEIGHT = 50
MIN_W_UNIT = 5
LINE_COLOR = "rgb(100, 100, 100)"
TEXT_COLOR = "black"
Y_STEP = 40
LINE_Y1 = 55
LINE_Y2_DIFF = 15
COLORS = ["red", "green", "blue", "yellow", "teal", "orange"]


class Rect(object):

    def __init__(self):
        self.x = None
        self.y = None
        self.width = None
        self.fill = None
        self.aux_attribs = [("height", str(RECT_HEIGHT))]


class Text(object):

    def __init__(self):
        self.x = None
        self.y = None
        self.probe_name = None
        self.ts = None
        self.aux_attribs = [("fill", TEXT_COLOR)]


class Line(object):

    def __init__(self):
        self.x1 = None
        self.x2 = None
        self.y1 = LINE_Y1
        self.y2 = None
        self.aux_attribs = [("stroke", LINE_COLOR)]


class TrackElement(object):

    def __init__(self):
        self.rect = None
        self.text = None
        self.line = None


class TrackDescription(object):

    def __init__(self):
        self.name = ""
        self.elements = []
        self.cur_x = 0
        self.cur_y = RECT_HEIGHT + 30
        self.log_line = None


class TracksDescription(object):

    def __init__(self):
        self.track_descriptions = []
        self.track_count = 0


class RequestReport(object):

    def __init__(self):
        self.tracks_descriptions = []
        self.request = None
        self.media_kind = None
        self.track_count = 0


def pcnt(c, tot):
    if tot == 0:
        return 0

    return round(c * 100. / tot, 2)


def build_track_description_elements(probes, times, state):
    state.with_blob = False
    track = {}

    try:
        W_UNIT = float(REPORT_WIDTH - LABEL_WIDTH) / max(times)
    except ZeroDivisionError:
        return

    def build_item(p, i, c, x, next_x, track_y):
        element = TrackElement()

        if p != -1:
            width = max(int((times[i] - times[p]) * W_UNIT), MIN_W_UNIT)

            if next_x > x + width:
                width = next_x - x

            element.rect = Rect()
            element.rect.x = x
            element.rect.y = track_y
            element.rect.width = width
            element.rect.fill = COLORS[c % len(COLORS)]
        else:
            width = 0

        next_x = x + width

        element.text = Text()
        element.text.x = next_x
        element.text.y = state.cur_y
        element.text.probe_name = probes[i].describe()
        element.text.ts = times[i]

        element.line = Line()
        element.line.x1 = next_x + 1
        element.line.y1 = track_y + LINE_Y1
        element.line.x2 = next_x + 1
        element.line.y2 = state.cur_y - LINE_Y2_DIFF

        state.elements.append(element)

        if probes[i].request in ["WriteBlob", "ReadBlob"]:
            state.with_blob = True

        return next_x

    def build_track(p, i, n, c, x, track_y):
        next_x = 0

        while i < n:
            if probes[i].name == "Join":
                _x = track[probes[i].span]
                _y = state.cur_y
                _n = min(i + int(probes[i].length) + 1, n)

                state.cur_y += RECT_HEIGHT + Y_STEP
                next_x = build_track(p, i + 1, _n, c, _x, _y) + MIN_W_UNIT
                i = _n

            else:
                if probes[i].name == "Fork":
                    track[probes[i].span] = x
                else:
                    x = build_item(p, i, c, x, next_x, track_y)
                    state.cur_y += Y_STEP
                    next_x = 0

                p = i
                i = i + 1
                c = c + 1

        return x

    state.cur_x = build_track(-1, 0, len(times), 0, 0, RECT_Y)


def build_track_description(request_report, label, probes, track_info):
    track_description = TrackDescription()
    track_description.log_line = track_info.get("L")
    times = track_info["T"]

    build_track_description_elements(probes, times, track_description)

    if track_description.with_blob:
        request_type = "WithReadBlob" if request_report.request == "ReadBlocks" else "WithWriteBlob"
    else:
        request_type = "WithoutReadBlob" if request_report.request == "ReadBlocks" else "WithoutWriteBlob"
    track_description.name = "%s %s (%s), %s" % (
        request_report.request, request_report.media_kind, request_type, label)

    return track_description


def build_track_description_markup(track_description, body):
    if track_description.name is not None:
        section_title = etree.SubElement(body, "h3")
        section_title.text = track_description.name

    if track_description.log_line is not None:
        section_debug = etree.SubElement(body, "div")
        section_debug.set("style", "color: grey; margin-top: 5px; margin-bottom: 5px;")
        section_debug.text = "Log Line: %s" % track_description.log_line

    section_image = etree.SubElement(body, "svg")
    section_image.set("width", str(track_description.cur_x + LABEL_WIDTH))
    section_image.set("height", str(track_description.cur_y))
    for element in track_description.elements:
        if element.rect is not None:
            rect = etree.SubElement(section_image, "rect")
            rect.set("x", str(element.rect.x))
            rect.set("y", str(element.rect.y))
            rect.set("width", str(element.rect.width))
            rect.set("fill", str(element.rect.fill))
            for x, y in element.rect.aux_attribs:
                rect.set(x, y)

        text = etree.SubElement(section_image, "text")
        text.set("x", str(element.text.x))
        text.set("y", str(element.text.y))
        text.text = "%s (%sus)" % (element.text.probe_name, element.text.ts)
        for x, y in element.text.aux_attribs:
            text.set(x, y)

        line = etree.SubElement(section_image, "line")
        line.set("x1", str(element.line.x1))
        line.set("y1", str(element.line.y1))
        line.set("x2", str(element.line.x2))
        line.set("y2", str(element.line.y2))
        for x, y in element.line.aux_attribs:
            line.set(x, y)


def build_report(rows):

    #
    #   Plotting
    #

    request_descr2request_report = defaultdict(RequestReport)

    for row in rows:
        request_and_media_kind = "%s_%s" % (row["Request"], media_kind2str(row["MediaKind"]))
        request_report = request_descr2request_report[request_and_media_kind]
        request_report.request = row["Request"]
        request_report.media_kind = media_kind2str(row["MediaKind"])

        tracks_description = TracksDescription()
        tracks_description.track_count += row.get("TrackCount", 0)
        request_report.track_count += tracks_description.track_count

        labels_and_track_info = sorted(
            [(label, track_info) for label, track_info in list(row["Times"].items())],
            key=lambda x: x[0]
        )

        for label, track_info in labels_and_track_info:
            if track_info.get("L"):
                probes = extract_minimized_trace(track_info.get("L")).probes
                break

        for label, track_info in labels_and_track_info:
            tracks_description.track_descriptions.append(
                build_track_description(request_report, label, probes, track_info)
            )

        request_report.tracks_descriptions.append(tracks_description)

    #
    #   Building markup
    #

    result = []

    for request_descr, request_report in list(request_descr2request_report.items()):
        html = etree.Element("html")
        body = etree.SubElement(html, "body")

        request_report.tracks_descriptions.sort(key=lambda x: -x.track_count)

        for tracks_description in request_report.tracks_descriptions:
            title = etree.SubElement(body, "h3")
            title.text = "TrackPercentage=%s (%s/%s)" % (
                pcnt(tracks_description.track_count, request_report.track_count),
                tracks_description.track_count,
                request_report.track_count
            )

            for track_description in tracks_description.track_descriptions:
                build_track_description_markup(track_description, body)

        result.append((request_descr, etree.tostring(html, pretty_print=True)))

    result.sort(key=lambda x: x[0])

    return result


def attach_trace(trace, body):
    times = [x.ts for x in trace.probes]
    track_description = TrackDescription()
    build_track_description_elements(trace.probes, times, track_description)
    build_track_description_markup(track_description, body)


def visualize_trace(trace):
    html = etree.Element("html")
    body = etree.SubElement(html, "body")
    attach_trace(trace, body)
    return etree.tostring(html, pretty_print=True)
