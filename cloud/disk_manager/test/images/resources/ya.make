PACKAGE()

FROM_SANDBOX(
    FILE
    2951476475
    RENAME RESOURCE
    OUT_NOAUTO qcow2_images/ubuntu-18.04-minimal-cloudimg-amd64.img
)

FROM_SANDBOX(
    FILE
    8091212496
    RENAME RESOURCE
    OUT_NOAUTO qcow2_images/ubuntu1604-ci-stable
)

FROM_SANDBOX(
    FILE
    6318551696
    RENAME RESOURCE
    OUT_NOAUTO vhd_images/ubuntu1604-ci-stable
)

FROM_SANDBOX(
    FILE
    4709742882
    RENAME RESOURCE
    OUT_NOAUTO vmdk_images/ubuntu-22.04-jammy-server-cloudimg-amd64.vmdk
)

FROM_SANDBOX(
    FILE
    5922914799
    RENAME RESOURCE
    OUT_NOAUTO vmdk_images/windows-vmdk-stream-optimised-multiple-grains.vmdk
)

END()
