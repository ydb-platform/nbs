import logging
from pathlib import Path

import pytest

from scripts.tracing.yatrace.evlog_loader import (
    MALFORMED_LINE_PREVIEW_CHARACTERS,
    _malformed_line_preview,
    load_ya_evlog,
)


def test_malformed_evlog_json_warns_once_with_bounded_example(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    middle = "not-useful-middle"
    malformed = "prefix:" + "a" * 700 + middle + "b" * 700 + ":suffix"
    evlog_path = tmp_path / "ya_evlog.jsonl"
    evlog_path.write_text(malformed + "\n{also-broken}\n")

    with caplog.at_level(
        logging.WARNING, logger="scripts.tracing.yatrace.evlog_loader"
    ):
        load_ya_evlog(evlog_path)

    records = [
        record
        for record in caplog.records
        if record.name == "scripts.tracing.yatrace.evlog_loader"
    ]
    assert len(records) == 1
    message = records[0].getMessage()
    assert str(evlog_path) in message
    assert "2 malformed JSON records" in message
    assert "line 1" in message
    assert "prefix:" in message
    assert ":suffix" in message
    assert "characters omitted" in message
    assert middle not in message

    preview = _malformed_line_preview(malformed)
    assert len(preview) == MALFORMED_LINE_PREVIEW_CHARACTERS
    assert preview.startswith("prefix:")
    assert preview.endswith(":suffix")
    assert middle not in preview
