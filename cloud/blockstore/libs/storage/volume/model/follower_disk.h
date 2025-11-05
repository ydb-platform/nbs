#pragma once

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Runtime disk status. ELeadershipStatus calculated based on the tag,
// TVolumeState::FollowerDisks, TVolumeState.LeaderDisks for the leader and
// follower disks.
enum class ELeadershipStatus
{
    // The disk is the principal. It is ready to serve the client's requests.
    // This is a normal condition for a disk that has no links.
    Principal,

    // The disk is a follower. Readings are prohibited. Can only accept writes
    // from the CopyVolumeClientId client.
    Follower,

    // This is a leader who has a follower to whom all the data has been
    // transferred. The transfer of leadership is currently taking place. All
    // read and write requests should be responded with the E_REJECTED error
    // code.
    LeadershipTransferring,

    // The disk has transferred leadership to follower. Responds to all
    // read/write requests with E_BS_INVALID_SESSION.
    LeadershipTransferred,
};

struct TLeaderFollowerLink
{
    TString LinkUUID;   // It can be empty if the exact uuid is not known.
    TString LeaderDiskId;
    TString LeaderShardId;
    TString FollowerDiskId;
    TString FollowerShardId;

    ui64 GetHash() const;
    TString LeaderDiskIdForPrint() const;
    TString FollowerDiskIdForPrint() const;
    TString Describe() const;

    bool Match(const TLeaderFollowerLink& rhs) const;
};

struct TOutdatedLeaderDestruction
{
    size_t TryCount = 0;
    TBackoffDelayProvider DelayProvider;
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
                       // Need to destroy previous leader.

        Principal = 30,   // Previous leader destroyed.
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

        DataReady = 40,   // All data transferred to follower.
                          // Need to transfer leadership to follower.
                          // Respond to all requests with the E_REJECTED code.

        LeadershipTransferred = 45,   // Leadership transferred to follower.
                                      // Respond to all requests with the
                                      // E_BS_INVALID_SESSION code.

        Error = 50,
    };

    TLeaderFollowerLink Link;
    TInstant CreatedAt;
    EState State = EState::None;
    NProto::EStorageMediaKind MediaKind =
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    std::optional<ui64> MigratedBytes;
    TString ErrorMessage;

    TString Describe() const;
    bool operator==(const TFollowerDiskInfo& rhs) const;
};

using TFollowerDisks = TVector<TFollowerDiskInfo>;

}   // namespace NCloud::NBlockStore::NStorage
