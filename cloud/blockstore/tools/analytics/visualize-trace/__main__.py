import importlib
import sys

from lxml import etree

if __name__ == '__main__':
    module = importlib.import_module(
        "cloud.blockstore.tools.analytics.find-perf-bottlenecks.lib")

    html = etree.Element("html")
    body = etree.SubElement(html, "body")
    n = 100
    i = 0

    def dump(i):
        with open("%04d.html" % i, "w") as f:
            f.write(etree.tostring(html, pretty_print=True).decode("utf8"))

    for line in sys.stdin:
        trace = module.extract_trace_from_log_message(line)

        if trace:
            module.attach_trace(trace, body)
            i += 1

        if i % n == 0 and len(body):
            dump(i / n)
            html = etree.Element("html")
            body = etree.SubElement(html, "body")

    if len(body):
        dump(i / n + 1)
