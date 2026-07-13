IF (NOT QEMU_BIN)
    DEPENDS(cloud/storage/core/tools/testing/qemu/bin)
ENDIF()

IF (NOT QEMU_FIRMWARE)
    DEPENDS(cloud/storage/core/tools/testing/qemu/bin)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tools/testing/qemu/bin/qemu-params.inc)

IF (NOT QEMU_ROOTFS)
    SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img)
    DEPENDS(cloud/storage/core/tools/testing/qemu/image-noble)
ENDIF()

IF (NOT QEMU_SSH_USER)
    SET(QEMU_SSH_USER qemu)
ENDIF()

IF (NOT QEMU_SSH_KEY)
    SET(QEMU_SSH_KEY cloud/storage/core/tools/testing/qemu/keys/id_rsa)
    DATA(arcadia/cloud/storage/core/tools/testing/qemu/keys)
ENDIF()

DEPENDS(
    cloud/storage/core/tests/recipes/qemu
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/virtiofs-server.inc)
SET(QEMU_USE_VIRTIOFS_SERVER True)

TAG(ya:dirty)

REQUIREMENTS(
    cpu:4
    ram:16
)

IF (QEMU_BIOS != "")
    SET(QEMU_BIOS_ARG --qemu-bios $QEMU_BIOS)
ENDIF()
IF (QEMU_CHARDEV_RECONNECT != "")
    SET(QEMU_CHARDEV_RECONNECT_ARG --chardev-reconnect $QEMU_CHARDEV_RECONNECT)
ENDIF()
IF (QEMU_VIRTIOFS_MIGRATION != "")
    SET(QEMU_VIRTIOFS_MIGRATION_ARG --virtiofs-migration $QEMU_VIRTIOFS_MIGRATION)
ENDIF()

USE_RECIPE(
    cloud/storage/core/tests/recipes/qemu/qemu-recipe
    --qemu-bin $QEMU_BIN
    --qemu-firmware $QEMU_FIRMWARE
    ${QEMU_BIOS_ARG}
    --qemu-options $QEMU_OPTIONS
    --rootfs $QEMU_ROOTFS
    --proc $QEMU_PROC
    --mem $QEMU_MEM
    --ssh-key $QEMU_SSH_KEY
    --ssh-user $QEMU_SSH_USER
    --virtio $QEMU_VIRTIO
    --enable-kvm $QEMU_ENABLE_KVM
    --instance-count $QEMU_INSTANCE_COUNT
    --invoke-test $QEMU_INVOKE_TEST
    --use-virtiofs-server $QEMU_USE_VIRTIOFS_SERVER
    --num-request-queues $QEMU_NUM_REQUEST_QUEUES
    ${QEMU_CHARDEV_RECONNECT_ARG}
    ${QEMU_VIRTIOFS_MIGRATION_ARG}
)

REQUIREMENTS(kvm)
