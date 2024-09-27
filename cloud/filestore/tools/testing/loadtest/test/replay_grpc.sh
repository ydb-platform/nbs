#!/usr/bin/env bash

set -ex

export FS=replay

CUR_DIR=$(readlink -e $(dirname $0))
. "${CUR_DIR}"/lib.sh

#ROOT_DIR=$(git rev-parse --show-toplevel)
export WORK_DIR=${CUR_DIR}/wrk
mkdir -p ${WORK_DIR}

pushd ${ROOT_DIR}
ya make -r cloud/filestore/tools/testing/loadtest/bin
popd

re_create_mount

# no need to mount

df -h ||:
mount | grep ${HOME} ||:

tmpl ${CUR_DIR}/replay_grpc.txt

#LOADTEST_VERBOSE=${VERBOSE=debug}
LOADTEST_VERBOSE=${VERBOSE=info}
env LD_LIBRARY_PATH=${ROOT_DIR}/cloud/filestore/tools/testing/loadtest/bin ${GDB} ${ROOT_DIR}/cloud/filestore/tools/testing/loadtest/bin/filestore-loadtest --verbose ${LOADTEST_VERBOSE} --tests-config ${CUR_DIR}/replay_grpc.txt 2>&1 | tee ${WORK_DIR}/log_grpc.log

pushd ${MOUNT_POINT}
find . -type f -iname "*" -printf "%h/%f %s \n" | sort | tee ${WORK_DIR}/replay_grpc_list.txt
popd

[ -z "${KEEP_MOUNT}" ] && umount ${MOUNT_POINT} ||:

diff ${WORK_DIR}/record_list.txt ${WORK_DIR}/replay_grpc_list.txt
