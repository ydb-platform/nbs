#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

class TOnlineRequestMonitor
{
private:
    struct TRequestMonInfo
    {
        TBlockRange64 Range;
        TInstant StartAt;
        TInstant FinishAt;
    };
    THashMap<ui64, TRequestMonInfo> Requests;

public:
    void RequestStarted(ui64 requestId, TBlockRange64 range);
    void RequestFinished(ui64 requestid);
    TString TakeRequestInfo();
};

}   // namespace NCloud::NBlockStore::NStorage
