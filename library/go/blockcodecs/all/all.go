package all

import (
	_ "github.com/ydb-platform/nbs/library/go/blockcodecs/blockbrotli"
	_ "github.com/ydb-platform/nbs/library/go/blockcodecs/blocklz4"
	_ "github.com/ydb-platform/nbs/library/go/blockcodecs/blocksnappy"
	_ "github.com/ydb-platform/nbs/library/go/blockcodecs/blockzstd"
)
