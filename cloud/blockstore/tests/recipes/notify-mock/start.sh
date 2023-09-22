#!/usr/bin/env bash
echo
echo Starting notify mock
echo

CTX_DIR=$1
PYTHONPATH=$source_root/ydb/public/sdk/python3:$source_root/library/python/testing/yatest_common:$source_root/library/python/testing:$source_root \
python3 $source_root/cloud/blockstore/tests/recipes/notify-mock/__main__.py start --output-dir $CTX_DIR --build-root "$build_root" --source-root "$source_root" --env-file $CTX_DIR/env.json --secure
code=$?
if [ $code -gt 0 ];then
  echo
  echo "Notify mock start failed"
  echo
  exit $code
fi

echo
echo Notify mock started successfully
echo
