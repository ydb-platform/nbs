#!/usr/bin/env python3

"""Prepare both cells' configurations and the second cell's working directory."""

from pathlib import Path
import re


# Only configured network ports change; tablet IDs and storage geometry stay intact.
PORTS = (29501, 9766, 9767, 9768, 9786, 9787, 9788, 10020)
PORT_PATTERN = re.compile(r'(?im)(\b\w*port\w*["\']?\s*:\s*)(\d+)\b')


def write_cells_config(directory, cell_id, peer_id, peer_port, icv2_port, peer_icv2_port):
    for node in range(2):
        suffix = "" if node == 0 else "2"
        (directory / "nbs" / f"nbs-cells{suffix}.txt").write_text(f'''CellsEnabled: true
CellId: "{cell_id}"

IcV2 {{
  Enabled: true
  EnableXdc: true
  ListenAddress: "127.0.0.1"
  Port: {icv2_port + node * 20}
}}

Cells {{
  CellId: "{peer_id}"
  GrpcPort: {peer_port}
  IcV2Port: {peer_icv2_port}
  Transport: CELL_DATA_TRANSPORT_GRPC
  Hosts {{
    Fqdn: "localhost"
  }}
}}
''')


def prepare_cells(example_dir):
    cell_dir = example_dir / "cell2"
    cell_dir.mkdir(exist_ok=True)
    for target in cell_dir.iterdir():
        if target.is_symlink() and not target.exists():
            target.unlink()

    for source in example_dir.iterdir():
        if source.is_symlink() or not source.is_file():
            continue
        if source.name.endswith("_cell2.sh") or source.name in ("0-setup.sh", "run-cell2.sh", "prepare_cells.py"):
            continue
        if source.suffix not in (".sh", ".py") and source.name != "encryption-key.txt":
            continue
        target = cell_dir / source.name
        if target.is_symlink():
            target.unlink()
        if not target.exists():
            target.symlink_to(Path("..") / source.name)

    for directory in ("static", "dynamic", "nbs"):
        target_dir = cell_dir / directory
        target_dir.mkdir(exist_ok=True)
        for source in (example_dir / directory).iterdir():
            if source.suffix not in (".txt", ".json", ".yaml"):
                continue
            if source.name.startswith(("nbs-disk-agent-", "nbs-location-", "nbs-disk-registry.")):
                continue
            if source.name in ("nbs-cells.txt", "nbs-cells2.txt"):
                continue
            text = PORT_PATTERN.sub(
                lambda match: match[1] + str(int(match[2]) + 10000)
                if int(match[2]) in PORTS else match[0], source.read_text())
            (target_dir / source.name).write_text(text)

    # Local service is implicit; each cell lists only the other cell as a peer.
    write_cells_config(example_dir, "cell-1", "cell-2", 19766, 29602, 39602)
    write_cells_config(cell_dir, "cell-2", "cell-1", 9766, 39602, 29602)


if __name__ == "__main__":
    prepare_cells(Path(__file__).resolve().parent)
