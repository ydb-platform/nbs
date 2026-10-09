#include "follower_disk.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/digest/multi.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString IdForPrint(const TString& diskId, const TString& cellId)
{
    return cellId ? (cellId + "/" + diskId) : diskId;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

ui64 TLeaderFollowerLink::GetHash() const
{
    return MultiHash(
        LeaderDiskId,
        LeaderCellId,
        FollowerDiskId,
        FollowerCellId);
}

TString TLeaderFollowerLink::LeaderDiskIdForPrint() const
{
    return IdForPrint(LeaderDiskId, LeaderCellId);
}

TString TLeaderFollowerLink::FollowerDiskIdForPrint() const
{
    return IdForPrint(FollowerDiskId, FollowerCellId);
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
    // A link persisted before cell ids were filled in has them empty.
    auto sameCell = [](const TString& a, const TString& b)
    {
        return !a || !b || a == b;
    };
    return LeaderDiskId == rhs.LeaderDiskId &&
           FollowerDiskId == rhs.FollowerDiskId &&
           sameCell(LeaderCellId, rhs.LeaderCellId) &&
           sameCell(FollowerCellId, rhs.FollowerCellId);
}

////////////////////////////////////////////////////////////////////////////////

TString TLeaderDiskInfo::Describe() const
{
    auto builder = TStringBuilder();
    builder << "{ State:" << ToString(State);

    if (ErrorMessage) {
        builder << ", ErrorMessage: " << ErrorMessage.Quote();
    }

    builder << " }";
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

    builder << ", MediaKind: " << NProto::EStorageMediaKind_Name(MediaKind);

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
