#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/common.sh"
ydb_sync_init

find contrib/ydb -type f \
    \( -name ya.make -o -name "*.make" -o -name "*.inc" -o -name "*.h" -o -name "*.cpp" -o -name "*.py" -o -name "*.proto" -o -name "*.jnj" -o -name "*.txt" \) \
    -print0 | xargs -0 perl -pi -e '
        s#contrib/ydb/library/contrib/ydb/library/yql#contrib/ydb/library/yql#g;
        s#yt/contrib/ydb/library/yql/providers#contrib/ydb/library/yql/providers#g;
        s#yql/essentials/#contrib/ydb/library/yql/#g;
        s#yql/providers/#contrib/ydb/library/yql/providers/#g;
        s#yt/yql/providers/#contrib/ydb/library/yql/providers/#g;
        s#yql\.essentials\.#contrib.ydb.library.yql.#g;
        s#yql\.providers\.#contrib.ydb.library.yql.providers.#g;
        s#yt\.yql\.providers\.#contrib.ydb.library.yql.providers.#g;
        s#RUN_PY3_PROGRAM#RUN_PROGRAM#g;
        s#NO_CLANG_MCDC_COVERAGE\(\)##g;
    '

find contrib/ydb -type f \( -name ya.make -o -name "*.h" -o -name "*.cpp" -o -name "*.inc" \) \
    -print0 | xargs -0 perl -pi -e 's#library/cpp/containers/absl(?!_flat_hash)#library/cpp/containers/absl_flat_hash#g'
