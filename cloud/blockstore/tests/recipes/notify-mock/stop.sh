#!/usr/bin/env bash
echo
echo Stopping notify mock
echo

CTX_DIR=$1
PYTHONPATH=$source_root/ydb/public/sdk/python3:$source_root/library/python/testing/yatest_common:$source_root/library/python/testing:$source_root \
python3 $source_root/cloud/blockstore/tests/recipes/notify-mock/__main__.py stop --output-dir $CTX_DIR --build-root "$build_root"
