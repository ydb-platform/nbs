#pragma once

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TLeaderFollowerLink
{
    TString LinkUUID; // It can be empty if the exact uuid is not known.
    TString LeaderDiskId;
    TString LeaderScaleUnitId;
    TString FollowerDiskId;
    TString FollowerScaleUnitId;

    TString LeaderDiskIdForPrint() const;
    TString FollowerDiskIdForPrint() const;
    TString Describe() const;

    bool Match(const TLeaderFollowerLink& rhs) const;
};

// Link info persisted on follower side.
struct TLeaderDiskInfo
{
    enum class EState
    {
        None = 0,
        Following = 10,   // The link has been created.
                          // and the disk is a follower.
                          // Disk accepts writes only from the leader.

        Leader = 20,   // The link is broken, the disk has become the new
                       // leader. All writes from old leader rejected.
    };

    TLeaderFollowerLink Link;
    TInstant CreatedAt;
    EState State = EState::None;
    TString ErrorMessage;

    TString Describe() const;

    bool operator==(const TLeaderDiskInfo& rhs) const;
};
using TLeaderDisks = TVector<TLeaderDiskInfo>;

// Link info persisted on leader side.
struct TFollowerDiskInfo
{
    enum class EState
    {
        None = 0,

        Created = 10,     // Persisted on leader side.
                          // Need to persist on follower side.

        Preparing = 30,   // Persisted on follower side.
                          // Preparing in progress.

        DataReady = 40,

        Error = 50,
    };

    TLeaderFollowerLink Link;
    TInstant CreatedAt;
    EState State = EState::None;
    std::optional<NProto::EStorageMediaKind> MediaKind;
    std::optional<ui64> MigratedBytes;
    TString ErrorMessage;

    TString Describe() const;
    bool operator==(const TFollowerDiskInfo& rhs) const;
};

using TFollowerDisks = TVector<TFollowerDiskInfo>;

struct TCreateFollowerRequestInfo
{
    TLeaderFollowerLink Link;
    NActors::TActorId CreateVolumeLinkActor;
    TVector<TRequestInfoPtr> Requests;
};

using TCreateFollowerRequests = TVector<TCreateFollowerRequestInfo>;

}   // namespace NCloud::NBlockStore::NStorage
