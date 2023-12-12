#!/usr/bin/env python3

from lxml import etree

import generate_report


def main():
    report = generate_report.build_report('test', './ut/logs/')

    # with open('./ut/actual.out', 'wb') as f:
    #     tree = etree.ElementTree(report)
    #     tree.write(f)

    loaded = etree.parse('./ut/expected.out')

    assert(etree.tostring(report) == etree.tostring(loaded))


if __name__ == '__main__':
    main()
