PACKAGE()

FROM_SANDBOX(
    FILE
    2951476475
    RENAME RESOURCE
    OUT_NOAUTO qcow2_images/ubuntu-18.04-minimal-cloudimg-amd64.img
)

FROM_SANDBOX(
    FILE
    3064742393
    RENAME RESOURCE
    OUT_NOAUTO qcow2_images/ubuntu1604-ci-stable
)

FROM_SANDBOX(
    FILE
    5274078903
    RENAME RESOURCE
    OUT_NOAUTO qcow2_images/panic.img
)

FROM_SANDBOX(
    FILE
    4709742882
    RENAME RESOURCE
    OUT_NOAUTO vmdk_images/ubuntu-22.04-jammy-server-cloudimg-amd64.vmdk
)

END()
