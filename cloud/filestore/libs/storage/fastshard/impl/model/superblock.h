#pragma once

#include "component.h"
#include "page_store.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////
// superblock layout

constexpr ui32 SuperBlockLayoutMinVersion = 1;
constexpr ui32 SuperBlockLayoutVersion = 1;

constexpr ui64 SuperBlockSlotSize = 4_KB;

struct TSuperBlockSlot
{
    ui64 LastNodeId;
    ui64 LastHandleId;
};

static_assert(sizeof(TSuperBlockSlot) <= SuperBlockSlotSize);
static_assert(SuperBlockSlotSize % alignof(TSuperBlockSlot) == 0);

////////////////////////////////////////////////////////////////////////////////

using TSuperBlockBase =
    TComponentBase<SuperBlockLayoutMinVersion, SuperBlockLayoutVersion>;
class TSuperBlock: public TSuperBlockBase
{
private:
    ui64 PageNo{};
    IPageStorePtr PageStore;

public:
    ui64 Init(ui64 firstPageNo, IPageStorePtr pageStore);

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return 1;
    }

    NProto::TError AllocateNodeId(TWriteContext& writeContext, ui64* nodeId);
    NProto::TError AllocateHandleId(
        TWriteContext& writeContext,
        ui64* handleId);

    [[nodiscard]] NProto::TError CollectStats(
        TFileSystemShardStats* stats) const;

private:
    NProto::TError AccessSlot(ui64 lsn, TBuffer& page, TSuperBlockSlot* slot);
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
