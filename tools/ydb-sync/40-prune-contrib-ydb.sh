#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/common.sh"
ydb_sync_init

paths=(
    docs
    requirements
    yql_docs
    apps/bridge_skipper_demo
    apps/etcd_proxy
    ci/debug
    ci/scripts
    core/counters_info
    core/local_indexes
    core/local_proxy
    core/memory_controller
    core/retro_tracing_impl
    core/split
    core/transfer
    deploy/prometheus
    library/analytics
    library/arrow_inference
    library/aws_init
    library/breakpad
    library/cloud_permissions
    library/drr
    library/error_tags
    library/global_plugins
    library/intersection_tree
    library/json_index
    library/kll_median
    library/plan2svg
    library/planner
    library/range_treap
    library/shop
    library/signals
    library/slide_limiter
    library/union_copy_set
    library/uring
    library/ut
    library/yql/parser/pg_wrapper/test
    library/yaml_json
    public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate
    services/bridge
    services/config
    services/nbs
    services/scheme_secret
    services/sqs_topic
    services/tablet
    services/test_shard
    services/udf_store
    tests/compatibility
    tests/datashard
    tests/example
    tests/hash_test
    tests/functional/ydb_cli/ai_interactive
    tests/solomon
    tests/sql
    tests/stability
    tests/stress
    tools/disk_obliterator
    tools/include_sanitizer
    tools/integrity_trails_helper
    tools/mnc
    tools/partcheck
    tools/stress_tool
    tools/tli_analysis
)

for path in "${paths[@]}"; do
    rm -rf "contrib/ydb/$path"
done

ai_tools_ymake=contrib/ydb/public/lib/ydb_cli/commands/interactive/ai/tools/ya.make
if [ -f "$ai_tools_ymake" ]; then
    perl -0pi -e '
        s#\# Embed YDB documentation archive when building release CLI binaries\.\n\# Build with `-DYDB_CLI_AI_INCLUDE_DOCS=yes` to enable\.\nIF\(YDB_CLI_AI_INCLUDE_DOCS\)\n    INCLUDE\(\$\{ARCADIA_ROOT\}/contrib/ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate/ya\.make\.inc\)\nENDIF\(\)\n\n##;
        s#\nRECURSE\(\n    docs_generate\n\)\n##;
    ' "$ai_tools_ymake"
fi

find contrib/ydb -type d -empty -delete
