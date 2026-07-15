from __future__ import annotations

from scripts.tests import finalize_workload_comments as fwc


def test_iter_full_build_presets_uses_target_platform() -> None:
    matrix_include = """
    {
      "include": [
        {"build_preset": "relwithdebinfo"},
        {"build_preset": "release-asan"},
        {
          "build_preset": "relwithdebinfo",
          "target_platform": "default-linux-armv9a_grace"
        }
      ]
    }
    """

    assert fwc.iter_full_build_presets(matrix_include, "linux-x86_64") == [
        "linux-arm64-relwithdebinfo",
        "linux-x86_64-release-asan",
        "linux-x86_64-relwithdebinfo",
    ]


def test_iter_full_build_presets_returns_empty_list_for_empty_matrix_output() -> None:
    assert fwc.iter_full_build_presets("", "linux-x86_64") == []
    assert fwc.iter_full_build_presets("  \n", "linux-x86_64") == []
