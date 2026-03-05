#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path
from xml.etree import ElementTree as ET


def save_suite(suite, path):
    root = ET.Element("testsuites")
    root.append(suite)
    tree = ET.ElementTree(root)
    tree.write(path)


def split_xml(fn, out_dir):
    try:
        tree = ET.parse(fn)
    except ET.ParseError as error:
        print(f"Unable to parse {fn}: {error}", file=sys.stderr)
        raise SystemExit(1) from error

    root = tree.getroot()

    for n, suite in enumerate(root.iter("testsuite")):
        part_fn = Path(out_dir).joinpath(f"part_{n}.xml")
        print(f"write {part_fn}")
        save_suite(suite, part_fn)


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", dest="out_dir", required=True)
    parser.add_argument("in_file", type=argparse.FileType("r"))
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not os.path.isdir(args.out_dir):
        os.makedirs(args.out_dir)

    split_xml(args.in_file, args.out_dir)


if __name__ == "__main__":
    main()
