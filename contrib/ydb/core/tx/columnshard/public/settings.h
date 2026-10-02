#pragma once

#include <contrib/ydb/core/control/immediate_control_board_impl.h>

#include <util/datetime/base.h>

namespace NKikimr::NColumnShard {

struct TSettings {
    static constexpr ui32 MAX_ACTIVE_COMPACTIONS = 1;

    static constexpr ui32 MAX_INDEXATIONS_TO_SKIP = 16;
    static constexpr TDuration GuaranteeIndexationInterval = TDuration::Seconds(10);
    static constexpr TDuration DefaultStatsReportInterval = TDuration::Seconds(10);
    static constexpr i64 GuaranteeIndexationStartBytesLimit = (i64)5 * 1024 * 1024 * 1024;

    TControlWrapper BlobWriteGrouppingEnabled;
    TControlWrapper CacheDataAfterIndexing;
    TControlWrapper CacheDataAfterCompaction;
    static constexpr ui64 OverloadTxInFlight = 1000;

    TSettings()
        : BlobWriteGrouppingEnabled(1, 0, 1)
        , CacheDataAfterIndexing(1, 0, 1)
        , CacheDataAfterCompaction(1, 0, 1) {
    }

    void RegisterControls(TControlBoard& icb) {
        icb.RegisterSharedControl(BlobWriteGrouppingEnabled, "ColumnShardControls.BlobWriteGrouppingEnabled");
        icb.RegisterSharedControl(CacheDataAfterIndexing, "ColumnShardControls.CacheDataAfterIndexing");
        icb.RegisterSharedControl(CacheDataAfterCompaction, "ColumnShardControls.CacheDataAfterCompaction");
    }
};

} // namespace NKikimr::NColumnShard
