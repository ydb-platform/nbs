#include "follower_disk.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString IdForPrint(const TString& diskId, const TString& scaleUnitId)
{
    return scaleUnitId ? (scaleUnitId + "/" + diskId) : diskId;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

TString TLeaderFollowerLink::LeaderDiskIdForPrint() const
{
    return IdForPrint(LeaderDiskId, LeaderScaleUnitId);
}

TString TLeaderFollowerLink::FollowerDiskIdForPrint() const
{
    return IdForPrint(FollowerDiskId, FollowerScaleUnitId);
}

TString TLeaderFollowerLink::Describe() const
{
    auto builder = TStringBuilder();
    builder << "[";
    builder << FollowerDiskIdForPrint().Quote();
    builder << " -> ";
    builder << LeaderDiskIdForPrint().Quote();
    builder << ", ";
    builder << LinkUUID.Quote();
    builder << "]";
    return builder;
}

bool TLeaderFollowerLink::Match(const TLeaderFollowerLink& rhs) const
{
    if (LinkUUID == rhs.LinkUUID) {
        return true;
    }
    if (LinkUUID && rhs.LinkUUID) {
        return false;
    }
    auto withoutUUID = [](const TLeaderFollowerLink& o)
    {
        return std::tie(
            o.LeaderDiskId,
            o.LeaderScaleUnitId,
            o.FollowerDiskId,
            o.FollowerScaleUnitId);
    };
    return withoutUUID(*this) == withoutUUID(rhs);
}

////////////////////////////////////////////////////////////////////////////////

TString TLeaderDiskInfo::Describe() const
{
    auto builder = TStringBuilder();
    builder << "{ State:" << ToString(State) << " }";
    return builder;
}

bool TLeaderDiskInfo::operator==(const TLeaderDiskInfo& rhs) const
{
    auto doTie = [](const TLeaderDiskInfo& o)
    {
        return std::tie(o.CreatedAt, o.State, o.ErrorMessage);
    };
    return Link.Match(rhs.Link) && doTie(*this) == doTie(rhs);
}

////////////////////////////////////////////////////////////////////////////////

TString TFollowerDiskInfo::Describe() const
{
    auto builder = TStringBuilder();
    builder << "{ State: " << ToString(State);

    if (MediaKind) {
        builder << ", MediaKind: "
                << NProto::EStorageMediaKind_Name(*MediaKind);
    }

    if (MigratedBytes) {
        builder << ", MigratedBytes: " << FormatByteSize(*MigratedBytes);
    }

    if (ErrorMessage) {
        builder << ", ErrorMessage: " << ErrorMessage.Quote();
    }

    builder << " }";
    return builder;
}

bool TFollowerDiskInfo::operator==(const TFollowerDiskInfo& rhs) const
{
    auto doTie = [](const TFollowerDiskInfo& o)
    {
        return std::tie(
            o.CreatedAt,
            o.State,
            o.MediaKind,
            o.MigratedBytes,
            o.ErrorMessage);
    };
    return Link.Match(rhs.Link) && doTie(*this) == doTie(rhs);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
