#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TChildLogTitle;

class TLogTitle
{
public:
    enum class EDetails
    {
        Brief,
        WithTime,
    };

    enum class EType
    {
        Volume,
        Partition,
        Session,
        Client,
        VolumeProxy,
    };

private:
    const EType Type;
    const TString SessionId;
    const TString ClientId;
    const ui64 StartTime;
    const ui32 PartitionIndex = 0;
    const ui32 PartitionCount = 0;
    const bool TemporaryServer = false;

    ui64 TabletId = 0;
    ui32 Generation = 0;
    TString DiskId;
    TString CachedPrefix;

public:
    // Constructor for Volume
    TLogTitle(ui64 tabletId, TString diskId, ui64 startTime);

    // Constructor for Partition
    TLogTitle(
        ui64 tabletId,
        TString diskId,
        ui64 startTime,
        ui32 partitionIndex,
        ui32 partitionCount);

    // Constructor for Session
    TLogTitle(
        EType type,
        TString sessionId,
        TString diskId,
        bool temporaryServer,
        ui64 startTime);

    // Constructor for Client
    TLogTitle(
        ui64 tabletId,
        TString sessionId,
        TString clientId,
        TString diskId,
        bool temporaryServer,
        ui64 startTime);

    // Constructor for VolumeProxy
    TLogTitle(TString diskId, bool temporaryServer, ui64 startTime);

    static TString
    GetPartitionPrefix(ui64 tabletId, ui32 partitionIndex, ui32 partitionCount);

    [[nodiscard]] TChildLogTitle GetChild(const ui64 startTime) const;

    [[nodiscard]] TString Get(EDetails details) const;

    [[nodiscard]] TString GetWithTime() const;
    [[nodiscard]] TString GetBrief() const;

    void SetDiskId(TString diskId);
    void SetGeneration(ui32 generation);
    void SetTabletId(ui64 tabletId);

private:
    void Rebuild();
    void RebuildForVolume();
    void RebuildForPartition();
    void RebuildForSession();
    void RebuildForClient();
    void RebuildForVolumeProxy();
    TString GetPartitionPrefix() const;
};

class TChildLogTitle
{
private:
    friend class TLogTitle;

    const TString CachedPrefix;
    const ui64 StartTime;

    TChildLogTitle(TString cachedPrefix, ui64 startTime);

public:
    [[nodiscard]] TString GetWithTime() const;
};

}   // namespace NCloud::NBlockStore::NStorage
