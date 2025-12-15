import cloud.filestore.tests.guest_cache.guest_cache_entry_timeout.lib \
    as timeout_utils


def test():
    timeout_utils.test_guest_cache_enty_timeout(
        expected_foo=0,
        expected_bar=0
        )
