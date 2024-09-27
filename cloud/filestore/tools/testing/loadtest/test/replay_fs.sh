#!/usr/bin/env bash

# Write to local fs:
# REPLAY_ROOT=tmp/tmp NO_MOUNT=1 ./replay_fs.sh

set -ex

export FS=replay

CUR_DIR=$(readlink -e $(dirname $0))
. "${CUR_DIR}"/lib.sh

export WORK_DIR=${CUR_DIR}/wrk
mkdir -p ${WORK_DIR}

pushd ${ROOT_DIR}
ya make -r  cloud/filestore/tools/testing/loadtest/bin 
popd

[ -z "${NO_MOUNT}" ] && re_create_mount

df -h ||:
mount | grep ${HOME} ||:

ls -la

export REPLAY_ROOT=${REPLAY_ROOT=${MOUNT_POINT}/replayed}
tmpl ${CUR_DIR}/replay_fs.txt

#LOADTEST_VERBOSE=${LT_VERBOSE=error}
LOADTEST_VERBOSE=${LT_VERBOSE=debug}
env LD_LIBRARY_PATH=${ROOT_DIR}/cloud/filestore/tools/testing/loadtest/bin ${GDB} ${ROOT_DIR}/cloud/filestore/tools/testing/loadtest/bin/filestore-loadtest --verbose ${LOADTEST_VERBOSE} --tests-config ${CUR_DIR}/replay_fs.txt 2>&1 | tee ${WORK_DIR}/log_fs.log

#popd

pushd ${REPLAY_ROOT}
find . -type f -iname "*" -printf "%h/%f %s \n" | sort | tee ${WORK_DIR}/replay_fs_list.txt
popd

[ -z "${KEEP_MOUNT}" ] && umount ${MOUNT_POINT} ||:

diff ${WORK_DIR}/record_list.txt ${WORK_DIR}/replay_fs_list.txt 
