#!/usr/bin/env bash

DATA_DIRS=" \
    certs \
    data \
    logs \
    "

PERSISTENT_TMP_DIR=${PERSISTENT_TMP_DIR:-$HOME/tmp/nbs}

for dir in $DATA_DIRS; do
    mkdir -p "$PERSISTENT_TMP_DIR/$dir"
    ln -svfT "$PERSISTENT_TMP_DIR/$dir" "$dir"
done

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

generate_cert "server" "certs"

DISK_KEY=2748 # 0xABC

format_disk() {
    DISK_GUID=$1
    DISK_SIZE=$2

    REAL_DISK_FILE=/var/tmp/pdisk-${DISK_GUID}.data
    DISK_FILE=data/pdisk-${DISK_GUID}.data

    rm -f $DISK_FILE
    rm -f $REAL_DISK_FILE
    dd if=/dev/zero of=$REAL_DISK_FILE bs=1 count=0 seek=$DISK_SIZE
    ln -s $REAL_DISK_FILE $DISK_FILE
}

format_disk ssd 64G
format_disk rot 64G

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ln -s "${SCRIPT_DIR}/static" static
ln -s "${SCRIPT_DIR}/dynamic" dynamic
ln -s "${SCRIPT_DIR}/nbs" nbs
