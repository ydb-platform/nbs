#!/usr/bin/env python3

# Render an SVG flamegraph from folded on/off-CPU samples
#
# Expected data file format: <stack> <on-cpu ns> <off-cpu ns>
#
# Build profiler binary:
#   git clone git@github.com:ClickHouse/silk.git
#   cd silk/src/profiler/
#   ../../bb -b release
#
# Prepare folded data samples:
#   PID=$(pidof filestore-vhost)
#   profiler --pid $PID --on-cpu --off-cpu --duration 30 > /tmp/profiler-$PID.dat
#
# Generate flamegraph:
#   python make_flamegraph.py --in /tmp/profiler-$PID.dat --out /tmp/flamegraph-$PID.svg \
#     --filter both --subtitle "generated on $(date)" --title "silk On+Off-CPU"

import argparse
import os
import subprocess
import sys
from contextlib import nullcontext
from enum import Enum
from pathlib import Path
import textwrap
from urllib.parse import quote

DEFAULT_SCRIPTS_DIR = str(Path.home() / "git" / "FlameGraph")
SCRIPTS_DIR = os.environ.get("SCRIPTS_DIR", DEFAULT_SCRIPTS_DIR)


class CpuFilter(str, Enum):
    BOTH = "both"
    ON_CPU = "on-cpu"
    OFF_CPU = "off-cpu"

    def __str__(self) -> str:
        return self.value


def _render_flamegraph(
    folded_file: str | None,
    out_svg: str | None,
    title: str,
    subtitle: str,
    sample_filter: CpuFilter,
    min_width: str,
) -> None:
    combined_lines: list[str] = []
    skipped_lines = 0
    input_name = folded_file or "stdin"

    input_context = open(folded_file) if folded_file else nullcontext(sys.stdin)
    with input_context as f:
        for line in f:
            parts = line.strip().rsplit(" ", 2)
            if len(parts) != 3:
                skipped_lines += 1
                continue
            stack, on_ns, off_ns = parts
            on_total = int(on_ns)
            off_total = int(off_ns)

            if sample_filter == CpuFilter.ON_CPU:
                total = on_total
            elif sample_filter == CpuFilter.OFF_CPU:
                total = off_total
            else:
                total = on_total + off_total

            if total > 0:
                combined_lines.append(f"{stack} {total}\n")

    if skipped_lines > 0:
        print(
            f"Warning: skipped {skipped_lines} malformed lines in folded file",
            file=sys.stderr,
        )
    if not combined_lines:
        raise ValueError(f"no valid samples found after filtering {input_name}")

    flamegraph_pl = os.path.join(SCRIPTS_DIR, "flamegraph.pl")
    output_context = open(out_svg, "w") if out_svg else nullcontext(sys.stdout)
    with output_context as outfile:
        subprocess.run(
            [
                flamegraph_pl,
                "--title",
                title,
                "--subtitle",
                subtitle,
                "--countname=ns",
                f"--minwidth={min_width}",
                "--hash",
            ],
            input="".join(combined_lines).encode(),
            stdout=outfile,
            check=True,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=textwrap.dedent("""
        Render an SVG flamegraph from folded on/off-CPU samples.
        Expected data file format: <stack> <on-cpu ns> <off-cpu ns>

        Prepare folded data samples:
          PID=$(pidof filestore-vhost)
          profiler --pid $PID --on-cpu --off-cpu --duration 30 > /tmp/profiler-$PID.dat

        Generate flamegraph:
          python make_flamegraph.py --in /tmp/profiler-$PID.dat --out /tmp/flamegraph-$PID.svg
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--in",
        help="Input folded stack file; if not specified, reads from stdin",
        dest="folded_file",
    )
    parser.add_argument(
        "--out",
        help="Output SVG path; if not specified, writes to standard output",
        dest="out_svg"
    )
    parser.add_argument(
        "--title",
        default="fg",
        help='Flamegraph title (default: "fg")',
    )
    parser.add_argument(
        "--subtitle",
        default="",
        help='Flamegraph subtitle (default: "")',
    )
    parser.add_argument(
        "--filter",
        type=CpuFilter,
        choices=tuple(CpuFilter),
        default=CpuFilter.BOTH,
        help="CPU sample filter: both sums on/off CPU samples (default: both)",
    )
    parser.add_argument(
        "--minwidth",
        default="0.1",
        help="Threshold to omit smaller functions"
    )
    parser.add_argument(
        "--urlpref",
        help="URL prefix to use for the printed flamegraph link",
    )
    return parser.parse_args()


def _output_uri(out_svg: str, urlpref: str | None) -> str:
    if not urlpref:
        out_path = Path(out_svg).expanduser().resolve()
        return out_path.as_uri()

    return f"{urlpref.rstrip('/')}/{quote(Path(out_svg).name)}"


if __name__ == "__main__":
    args = _parse_args()
    _render_flamegraph(
        args.folded_file,
        args.out_svg,
        args.title,
        args.subtitle,
        args.filter,
        args.minwidth,
    )

    # Show clickable link
    # If you want clicks to open Edge browser, run this:
    # xdg-mime default microsoft-edge.desktop image/svg+xml
    if args.out_svg:
        print(f"{_output_uri(args.out_svg, args.urlpref)}", file=sys.stderr)
