IF (NOT QEMU_BIN)
    DEPENDS(cloud/storage/core/tools/testing/qemu/bin)
ENDIF()

IF (NOT QEMU_FIRMWARE)
    DEPENDS(cloud/storage/core/tools/testing/qemu/bin)
ENDIF()

IF (NOT QEMU_ROOTFS)
    SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image/rootfs.img)
    DEPENDS(cloud/storage/core/tools/testing/qemu/image)
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

USE_RECIPE(
    cloud/storage/core/tests/recipes/qemu/qemu-recipe
    --qemu-bin $QEMU_BIN
    --qemu-firmware $QEMU_FIRMWARE
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
)

REQUIREMENTS(kvm)
