PY3_PROGRAM(mnc)

    SUBSCRIBER(kruall)

    PY_MAIN(contrib.ydb.tools.mnc.cli.main)

    PY_SRCS(
        arg_metadata.py
        command_options.py
        main.py
        parser_factory.py
        tui/__init__.py
        tui/app.py
        tui/command_picker.py
        tui/common.py
        tui/config_picker.py
        tui/launcher.py
        tui/options_form.py

        commands/__init__.py
        commands/install.py
        commands/uninstall.py
        commands/disks.py
        commands/agent.py
        commands/configs.py
        commands/init.py
        commands/deploy.py
        commands/service.py
        commands/nbs.py
        commands/qemu.py
    )

    PEERDIR(
        contrib/python/PyYAML
        contrib/python/aiohttp
        contrib/python/rich
        contrib/ydb/apps/dstool/lib
        contrib/ydb/tools/ydbd_slice
        contrib/ydb/tests/library/clients
        contrib/ydb/tests/library
        contrib/ydb/tools/mnc/lib
        contrib/python/requests
        contrib/python/textual
    )

END()

RECURSE_FOR_TESTS(
    ut
)
