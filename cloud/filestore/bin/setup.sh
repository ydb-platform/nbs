#!/usr/bin/env bash

abspath() {
    path=$1

    if [ ! -d $path ]; then
        echo $path
        return
    fi

    (cd $1; pwd)
}

# arguments: $1: build directory
# $2 - $n: files to check
build_dir_mtime() {
    build_dir=$1
    shift
    build_files=$@

    max_mtime=0
    for f in $build_files
    do
        if [ ! -f $build_dir/$f ]; then
            echo 0
            return
        fi

        mtime=`stat -c "%Y" $build_dir/$f`
        if [ $mtime -gt $max_mtime ]; then
            max_mtime=$mtime
        fi
    done

    echo $max_mtime
}

# arguments: $1: arcadia directory
# $2 - $n: files to find
find_build_dir() {
    arcadia_dir=$1
    build_root_candidates=`echo $arcadia_dir/../ybuild $arcadia_dir/../ybuild/* $arcadia_dir/../build $arcadia_dir/ybuild`
    shift

    max_mtime=0
    max_build_dir=""

    for build_root in $build_root_candidates
    do
        [ -d $build_root ] || continue

        for build_type in "release" "debug"
        do
            build_dir=`abspath $build_root/$build_type`
            [ -d $build_dir ] || continue

            mtime=`build_dir_mtime $build_dir $@`
            if [ $mtime -gt 0 -a $mtime -gt $max_mtime ]; then
                max_mtime=$mtime
                max_build_dir=$build_dir
            fi
        done
    done

    #if not yet found try to check arcadia dir itself
    if [ -z "$max_build_dir" ]; then
        max_build_dir=$arcadia_dir
        for f in $@; do
            [ -e $arcadia_dir/$f ] || max_build_dir=""
        done
    fi

    echo $max_build_dir
}

# locate arcadia directory and check configuration file presence
find_arcadia_dir() {
    readlink -e `dirname $0`/../../../
}

find_bin_dir() {
    readlink -e `dirname $0`
}

generate_cert() {
    local pass="pass123"
    local name="$1"
    local fallback_name="$name-fallback"
    local dir="$2"
    openssl genrsa -passout pass:$pass -des3 -out $dir/$name.key 4096
    openssl req -passin pass:$pass -new -key $dir/$name.key -out $dir/$name.csr -subj "/C=RU/L=SaintPetersburg/O=Yandex/OU=Infrastructure/CN=$(hostname --fqdn)"
    openssl req -passin pass:$pass -new -key $dir/$name.key -out $dir/$fallback_name.csr -subj "/C=RU/L=SaintPetersburg/O=Yandex/OU=Infrastructure/CN=localhost"
    openssl x509 -req -passin pass:$pass -days 365 -in $dir/$name.csr -signkey $dir/$name.key -set_serial 01 -out $dir/$name.crt
    openssl x509 -req -passin pass:$pass -days 365 -in $dir/$fallback_name.csr -signkey $dir/$name.key -set_serial 01 -out $dir/$fallback_name.crt
    openssl rsa -passin pass:$pass -in $dir/$name.key -out $dir/$name.key
    rm $dir/$name.csr $dir/$fallback_name.csr
}

SYMLINK_BINARIES=${SYMLINK_BINARIES:-true}

ARCADIA_DIR=`find_arcadia_dir`
BIN_DIR=`find_bin_dir`

echo "${SYMLINK_BINARIES}"

BUILD_FILES=" \
    cloud/filestore/apps/client/filestore-client                            \
    cloud/filestore/apps/server/filestore-server                            \
    cloud/filestore/apps/vhost/filestore-vhost                              \
    cloud/nbs_internal/filestore/gateway/nfs/server/filestore-nfs           \
    cloud/nbs_internal/filestore/tools/generate-packages/generate-packages  \
    cloud/storage/core/tools/ops/config_generator/config_generator          \
    kikimr/public/tools/package/stable/Berkanavt/kikimr/bin/kikimr          \
    kikimr/tools/cfg/bin/kikimr_configure                                   \
    "

if $SYMLINK_BINARIES; then
    # If SYMLINK_BINARIES is set to false, it is assumed that binaries are already
    # in $BIN_DIR. Otherwise current script will try to locate proper binaries and
    # add symlinks from $BIN_DIR to them.

    BUILD_DIR=`find_build_dir $ARCADIA_DIR $BUILD_FILES`

    if [ -z "$BUILD_DIR" ]; then
        echo "ERROR: No suitable build directory found"
        exit 2
    fi

    # create symlinks
    for file in $BUILD_FILES; do
        ln -svf $BUILD_DIR/$file $BIN_DIR/
    done
else
    for file in $BUILD_FILES; do
        basename=`basename $file`
        if [ ! -f $BIN_DIR/$basename ]; then
            echo "WARNING: $BIN_DIR/$basename not found"
        fi
    done
fi

DATA_DIRS=" \
    certs \
    data \
    dynamic \
    mount \
    nfs \
    static \
    "

PERSISTENT_TMP_DIR="$HOME/.tmp/nfs"

# Place directories outside arc tree, because arc FS has a trouble with large files.
for dir in $DATA_DIRS; do
    mkdir -p "$PERSISTENT_TMP_DIR/$dir"
    ln -svfT "$PERSISTENT_TMP_DIR/$dir" "$BIN_DIR/$dir"
done

# Generate certs
generate_cert "server" "$BIN_DIR/certs"

# hide binaries from svn
svn info &>/dev/null && svn propset svn:ignore -F $BIN_DIR/.svnignore $BIN_DIR || true
