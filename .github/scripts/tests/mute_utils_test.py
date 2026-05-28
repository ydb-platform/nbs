from __future__ import annotations

import xml.etree.ElementTree as ET

from scripts.tests import mute_utils as mu


def test_pattern_to_re_handles_wildcard() -> None:
    regex = mu.pattern_to_re("abc*def")
    assert regex.startswith("(?:^")
    assert regex.endswith("$)")


def test_mute_target_replaces_failure_with_skipped_and_property() -> None:
    case = ET.Element("testcase")
    failure = ET.SubElement(case, "failure", {"message": "boom"})
    failure.text = "details"

    changed = mu.mute_target(case)

    assert changed is True
    assert case.find("failure") is None
    skipped = case.find("skipped")
    assert skipped is not None
    assert skipped.get("message") == "boom"

    props = case.find("properties")
    assert props is not None
    names = [item.get("name") for item in props.findall("property")]
    assert "mute" in names
