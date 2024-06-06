#pragma once

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>

#include <array>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TRequestTypeStats
{
    std::atomic<ui64> Count = 0;
    std::atomic<ui64> RequestBytes = 0;
    std::atomic<ui32> Inflight = 0;
    std::atomic<ui32> InflightBytes = 0;
    std::array<
        std::atomic<ui64>,
        static_cast<ui32>(EDiagnosticsErrorKind::Max)
    > ErrorKind2Count = {};

    std::atomic<ui64>& E(EDiagnosticsErrorKind errorKind)
    {
        return ErrorKind2Count[static_cast<ui32>(errorKind)];
    }

    ui64 E(EDiagnosticsErrorKind errorKind) const
    {
        return const_cast<TRequestTypeStats*>(this)->E(errorKind);
    }
};

using TRequestTypeStatsArray = std::array<
    TRequestTypeStats,
    static_cast<ui32>(EBlockStoreRequest::MAX)>;

struct IProxyRequestStats: IRequestStats
{
    [[nodiscard]]
    virtual const TRequestTypeStatsArray& GetInternalStats() const = 0;
};

using IProxyRequestStatsPtr = std::shared_ptr<IProxyRequestStats>;

////////////////////////////////////////////////////////////////////////////////

IProxyRequestStatsPtr CreateProxyRequestStats();
IStoragePtr CreateProxyStorage(
    IBlockStorePtr blockStore,
    IRequestStatsPtr requestStats,
    ui32 blockSize);

}   // namespace NCloud::NBlockStore::NServer
