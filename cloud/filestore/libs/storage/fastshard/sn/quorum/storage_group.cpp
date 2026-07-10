#include "storage_group.h"

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageGroupImpl: public IStorageGroup
{
private:
    TVector<IStorageNodePtr> Nodes;

public:
    explicit TStorageGroupImpl(TVector<IStorageNodePtr> nodes)
        : Nodes(std::move(nodes))
    {}

public:
    NProto::TAcquireDevicesResponse AcquireDevices(
        NProto::TAcquireDevicesRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TReleaseDevicesResponse ReleaseDevices(
        NProto::TReleaseDevicesRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TWriteLogRecordResponse WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TReadPagesResponse ReadPages(
        NProto::TReadPagesRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<IStorageNodePtr> nodes)
{
    return std::make_shared<TStorageGroupImpl>(std::move(nodes));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
