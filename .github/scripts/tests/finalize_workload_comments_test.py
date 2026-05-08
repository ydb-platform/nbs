from __future__ import annotations

from scripts.tests import finalize_workload_comments as fwc


def test_iter_build_presets_returns_unique_sorted_values() -> None:
    matrix_include = """
    {
      "include": [
        {"build_preset": "relwithdebinfo"},
        {"build_preset": "release-asan"},
        {"build_preset": "relwithdebinfo"}
      ]
    }
    """

    assert fwc.iter_build_presets(matrix_include) == [
        "release-asan",
        "relwithdebinfo",
    ]
