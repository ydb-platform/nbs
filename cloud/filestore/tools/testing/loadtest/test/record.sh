#!/usr/bin/env bash

set -ex

export FS=record

CUR_DIR=$(readlink -e $(dirname $0))
. "${CUR_DIR}"/lib.sh

export WORK_DIR=${CUR_DIR}/wrk
mkdir -p ${WORK_DIR}

re_create_mount

df -h ||:
mount | grep ${HOME} ||:

pushd ${MOUNT_POINT}

# ${CUR_DIR}/load1.sh ||:
# ${CUR_DIR}/load2.sh ||:
 ${CUR_DIR}/load3.sh ||:

find .  -type f -iname "*" -printf "%h/%f %s \n" | sort | tee ${WORK_DIR}/record_list.txt

popd


sleep 15

ls -la ${LOGS_DIR}/filestore-server-profile-log.txt
cp ${LOGS_DIR}/filestore-server-profile-log.txt ${WORK_DIR}

[ -z "${KEEP_MOUNT}" ] && umount ${MOUNT_POINT} ||:
