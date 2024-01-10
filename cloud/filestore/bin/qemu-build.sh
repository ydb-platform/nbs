#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

abspath() {
    path=$1

    if [ ! -d $path ]; then
        echo $path
        return
    fi

    (cd $1; pwd)
}

BIN_DIR=`find_bin_dir`

SRC_DIR=`abspath ${QEMU_TREE:-$BIN_DIR/../../../../../qemu}`
BUILD_DIR=$SRC_DIR/build

mkdir -p $BUILD_DIR
pushd $BUILD_DIR

git pull --recurse

EXTRA_CFLAGS=" \
    -g \
    -O2 \
    -fstack-protector-strong \
    -Wformat \
    -Werror=format-security \
    -Wdate-time \
    -D_FORTIFY_SOURCE=2 \
    -DCONFIG_QEMU_DATAPATH='\"/usr/share/qemu:/usr/share/seabios:/usr/lib/ipxe/qemu\"' \
    -DVENDOR_UBUNTU \
    "

EXTRA_LDFLAGS=" \
    -Wl,-Bsymbolic-functions \
    -Wl,-z,relro \
    -Wl,--as-needed \
    "

PREFIXES=" \
    --prefix=/usr \
    --interp-prefix=/etc/qemu-binfmt/%M \
    --libdir=/usr/lib/x86_64-linux-gnu \
    --libexecdir=/usr/lib/qemu \
    --localstatedir=/var \
    --localstatedir=/var \
    --sysconfdir=/etc \
    "

COMPONENTS=" \
    --enable-attr \
    --enable-cap-ng \
    --enable-curl \
    --enable-fdt \
    --enable-gnutls \
    --enable-guest-agent \
    --enable-kvm \
    --enable-linux-aio \
    --enable-modules \
    --enable-seccomp \
    --enable-system \
    --enable-tools \
    --enable-trace-backends=simple,log \
    --enable-vhost-net \
    --enable-vhost-user \
    --enable-vhost-user-fs \
    --enable-virtfs \
    --enable-vnc \
    --enable-vnc-jpeg \
    --enable-vnc-png \
    --enable-vnc-sasl \
    --enable-xfsctl \
    --disable-blobs \
    --disable-brlapi \
    --disable-libiscsi \
    --disable-libusb \
    --disable-linux-user \
    --disable-rbd \
    --disable-rdma \
    --disable-smartcard \
    --disable-spice \
    --disable-strip \
    --disable-usb-redir \
    --disable-user \
    --disable-vte \
    --disable-xen \
    "
    # --disable-bluez \
    # --disable-curses \
    # --disable-gtk \
    # --disable-sdl \

../configure \
    --target-list=x86_64-softmmu \
    --extra-cflags="$EXTRA_CFLAGS" \
    --extra-ldflags="$EXTRA_LDFLAGS" \
    $PREFIXES \
    $COMPONENTS \

make -j 16

popd

ln -svf $BUILD_DIR/x86_64-softmmu/qemu-system-x86_64 $BIN_DIR/
ln -svf $BUILD_DIR/qemu-img $BIN_DIR/
ln -svf $BUILD_DIR/qemu-nbd $BIN_DIR/
ln -svf $BUILD_DIR/tools/virtiofsd/virtiofsd $BIN_DIR/
