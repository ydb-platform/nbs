#!/usr/bin/env python3

import os
import subprocess
import sys

SCRIPTS_DIR = "/home/astr/git/FlameGraph"

def _render_flamegraph(folded_file: str, out_svg: str, title: str) -> None:
    combined_lines: list[str] = []

    with open(folded_file) as f:
        for line in f:
            parts = line.strip().split(" ")
            if len(parts) != 3:
                continue
            stack, on_ns, off_ns = parts
            total = int(on_ns) + int(off_ns)
            if total > 0:
                combined_lines.append(f"{stack} {total}\n")

    flamegraph_pl = os.path.join(SCRIPTS_DIR, "flamegraph.pl")
    with open(out_svg, "w") as outfile:
        subprocess.run(
            [flamegraph_pl, "--title", title, "--countname=ns", "--hash"],
            input="".join(combined_lines).encode(),
            stdout=outfile,
            check=True,
        )

_render_flamegraph(sys.argv[1], sys.argv[2], "fg")
