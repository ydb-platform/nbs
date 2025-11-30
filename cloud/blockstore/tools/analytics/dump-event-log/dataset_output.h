#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <cloud/storage/core/libs/common/ring_buffer.h>

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/stream/file.h>

namespace NCloud::NBlockStore {

class TDatasetOutput
{
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TDatasetOutput(const TString& filename);
    ~TDatasetOutput();

    void ProcessRequests(const NProto::TProfileLogRecord& record);
};

}   // namespace NCloud::NBlockStore
