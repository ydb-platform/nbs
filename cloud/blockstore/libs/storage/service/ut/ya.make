# The storage service tests pull in the full storage test runtime and YDB testlib.
# With UBSAN this currently produces binaries that exceed x86_64 PC-relative
# relocation limits during linking. Keep these tests enabled for other builds.
IF (SANITIZER_TYPE != "undefined")
    RECURSE_FOR_TESTS(
        state
        actions
        alter
        create_from_device
        create
        describe_model
        describe
        destroy
        forward
        inactive_clients
        link_volume
        list
        manually_preempted_volumes
        mount
        placement
        read_write
        resume_device
        start
        stats
        update_config
        vhost_discard
    )
ENDIF()
