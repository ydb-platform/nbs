#!/usr/bin/env bash

# locate arcadia directory and check configuration file presence
find_repo_dir() {
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
    openssl req -passin pass:$pass -new -key $dir/$name.key -out $dir/$name.csr -subj "/C=RU/L=MyCity/O=MyCloud/OU=Infrastructure/CN=$(hostname --fqdn)"
    openssl req -passin pass:$pass -new -key $dir/$name.key -out $dir/$fallback_name.csr -subj "/C=RU/L=MyCity/O=MyCloud/OU=Infrastructure/CN=localhost"
    openssl x509 -req -passin pass:$pass -days 365 -in $dir/$name.csr -signkey $dir/$name.key -set_serial 01 -out $dir/$name.crt
    openssl x509 -req -passin pass:$pass -days 365 -in $dir/$fallback_name.csr -signkey $dir/$name.key -set_serial 01 -out $dir/$fallback_name.crt
    openssl rsa -passin pass:$pass -in $dir/$name.key -out $dir/$name.key
    rm $dir/$name.csr $dir/$fallback_name.csr
}

REPO_DIR=`find_repo_dir`
BIN_DIR=`find_bin_dir`

BUILD_ROOT="$REPO_DIR/cloud/filestore/buildall"
YDBD_BIN="ydb/apps/ydbd/ydbd"
FILESTORE_CLIENT_BIN="cloud/filestore/apps/client/filestore-client"
FILESTORE_SERVER_BIN="cloud/filestore/apps/server/filestore-server"
FILESTORE_VHOST_BIN="cloud/filestore/apps/vhost/filestore-vhost"


# create symlinks
for file in $YDBD_BIN $FILESTORE_CLIENT_BIN $FILESTORE_SERVER_BIN $FILESTORE_VHOST_BIN; do
    ln -svf $BUILD_ROOT/$file $BIN_DIR/
done

for file in $BUILD_FILES; do
    basename=`basename $file`
    if [ ! -f $BIN_DIR/$basename ]; then
        echo "WARNING: $BIN_DIR/$basename not found"
    fi
done


DATA_DIRS=" \
    certs \
    data \
    mount \
    "

PERSISTENT_TMP_DIR=${PERSISTENT_TMP_DIR:-$HOME/tmp/nfs}

# Place directories outside git tree
for dir in $DATA_DIRS; do
    mkdir -p "$PERSISTENT_TMP_DIR/$dir"
    ln -svfT "$PERSISTENT_TMP_DIR/$dir" "$BIN_DIR/$dir"
done

# Generate certs
generate_cert "server" "$BIN_DIR/certs"
