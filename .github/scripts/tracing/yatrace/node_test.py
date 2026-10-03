import pytest

from .node import YaNode, _chunk_index


@pytest.mark.parametrize(
    ("output", "expected"),
    [
        ("suite/test-results/unittest/chunk3/ytest.report.trace", 3),
        ("suite/test-results/unittest/chunk0/ytest.report.trace", 0),
        ("suite/test-results/unittest/run1/chunk3/ytest.report.trace", 3),
        (
            "suite/test-results/unittest/linux-x86_64/run1/test_file/chunk3/"
            "ytest.report.trace",
            3,
        ),
        (
            "suite/test-results/unittest/testing_out_stuff/1/chunk3/"
            "ytest.report.trace",
            3,
        ),
        ("suite/test-results/unittest/test_file/chunk12/ytest.report.trace", 12),
        ("suite/test-results/unittest/chunk7/chunk3/ytest.report.trace", 3),
        ("suite/test-results/unittest/ytest.report.trace", None),
        ("suite/test-results/chunk3/ytest.report.trace", None),
        ("suite/test-results/unittest/chunk3", None),
        ("suite/test-results/unittest/chunk3.tar", None),
        ("suite/test-results/unittest/chunkx/ytest.report.trace", None),
        ("suite/chunk3/result.o", None),
    ],
)
def test_chunk_index_follows_test_work_directory_layout(
    output: str, expected: int | None
) -> None:
    assert _chunk_index((output,)) == expected


def test_chunk_index_skips_outputs_without_a_chunk() -> None:
    assert (
        _chunk_index(
            (
                "suite/result.o",
                "suite/test-results/unittest/ytest.report.trace",
                "suite/test-results/unittest/chunk3/ytest.report.trace",
            )
        )
        == 3
    )


def test_node_keeps_test_identity_separate_from_chunk_index() -> None:
    node = YaNode.from_raw(
        {
            "name": (
                "Run(uid$(BUILD_ROOT)/suite/test-results/unittest/"
                "chunk3/ytest.report.trace)"
            ),
            "tag": "TM",
            "time": [1, 2],
        }
    )

    assert node is not None
    assert node.test_identity == ("suite", "unittest")
    assert node.test_chunk_index == 3
