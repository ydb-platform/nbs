#!/usr/bin/env python3
from __future__ import annotations

import argparse

from .generate_summary import gen_summary_counts, render_summary_markdown


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--title", default="GA Scripts pytest")
    parser.add_argument("results_xml")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    summary = gen_summary_counts([(args.title, args.results_xml)])
    markdown = render_summary_markdown(summary)
    if markdown:
        print(markdown)


if __name__ == "__main__":
    main()
