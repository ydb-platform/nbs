def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "nbs_config(nbd_request_timeout=10, max_zero_blocks_sub_request_size=None): "
        "configure the NBS test environment",
    )
