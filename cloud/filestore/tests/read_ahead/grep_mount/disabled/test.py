import cloud.filestore.tests.read_ahead.grep_mount.lib as grep_mount


def test_grep_read_ahead_disabled():
    grep_mount.run_grep_read_ahead_benchmark("disabled")
