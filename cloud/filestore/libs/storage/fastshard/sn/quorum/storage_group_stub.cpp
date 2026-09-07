#include "storage_group.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageGroupStub: public IStorageGroup
{
public:
    NProto::TError Init() override
    {
        return MakeError(E_NOT_IMPLEMENTED);
    }

    void TearDown() override
    {}

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        ui64 lsn) override
    {
        Y_UNUSED(headers, pageGroups, lsn);
        return MakeError(E_NOT_IMPLEMENTED);
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        Y_UNUSED(headers, pageGroupRefs, pageGroups);
        return MakeError(E_NOT_IMPLEMENTED);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TStorageGroupConfig config,
    TVector<TStorageDevice> devices,
    ITimerPtr timer)
{
    Y_UNUSED(config, devices, timer);

    return std::make_shared<TStorageGroupStub>();
}

ITimerPtr CreateFiberTimer()
{
    return CreateWallClockTimer();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
