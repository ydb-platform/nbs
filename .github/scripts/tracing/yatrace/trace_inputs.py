"""Discover and describe input files for a ya trace report."""

from __future__ import annotations

import logging
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from .trace_loader import _load_ya_trace_files

if TYPE_CHECKING:
    from .trace_collection import YaTraceCollection


LOGGER = logging.getLogger(__name__)


@dataclass
class YaTraceInputs:
    root: Path
    trace_paths: list[Path]
    evlog_path: Path | None = None

    @classmethod
    def discover(
        cls,
        root: Path,
        *,
        evlog_path: Path | None = None,
        modified_since: float | None = None,
    ) -> YaTraceInputs:
        root = root.resolve()
        trace_paths = []
        for path in sorted(root.rglob("ytest.report.trace")):
            try:
                relative = path.relative_to(root)
                path_stat = path.lstat()
                resolved = path.resolve(strict=True)
            except (OSError, ValueError) as error:
                LOGGER.warning("Skipping ya trace input %s: %s", path, error)
                continue
            if not stat.S_ISREG(path_stat.st_mode) or resolved != root / relative:
                LOGGER.warning("Skipping unsafe ya trace input: %s", path)
                continue
            if modified_since is not None and path_stat.st_mtime < modified_since:
                continue
            trace_paths.append(path)

        safe_evlog_path = None
        if evlog_path is not None:
            try:
                evlog_stat = evlog_path.lstat()
            except FileNotFoundError:
                LOGGER.warning("Ya event log does not exist: %s", evlog_path)
            except OSError as error:
                LOGGER.warning(
                    "Unable to inspect ya event log %s: %s",
                    evlog_path,
                    error,
                )
            else:
                if stat.S_ISREG(evlog_stat.st_mode):
                    safe_evlog_path = evlog_path
                else:
                    LOGGER.warning("Skipping unsafe ya event log: %s", evlog_path)

        return cls(
            root=root,
            trace_paths=trace_paths,
            evlog_path=safe_evlog_path,
        )

    def bundle_manifest(
        self,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        evlog_archive_name = (
            self.evlog_path.name if self.evlog_path is not None else None
        )
        if evlog_archive_name in {"trace-inputs.manifest.json", "ya-out"}:
            raise ValueError(
                "Ya event log name conflicts with a reserved bundle member"
            )
        return {
            "schema": "nbs-ya-trace-input-bundle",
            "schema_version": 1,
            "evlog_file": evlog_archive_name,
            "ya_out_dir": "ya-out",
            "ya_trace_file_count": len(self.trace_paths),
            "metadata": dict(metadata or {}),
        }

    def tar_file_list(self) -> bytes:
        """Return NUL-delimited trace paths relative to the ya output root."""
        return b"".join(
            b"./" + os.fsencode(trace_path.relative_to(self.root)) + b"\0"
            for trace_path in self.trace_paths
        )

    def parse(self) -> YaTraceCollection:
        from .trace_collection import YaTraceCollection

        return YaTraceCollection(
            root=self.root,
            traces=_load_ya_trace_files(self.root, self.trace_paths),
        )
