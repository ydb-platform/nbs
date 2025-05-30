#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>
namespace NCloud::NBlockStore::NStorage {

class TLogTitle
{
public:
    enum class EDetails
    {
        Brief,
        WithTime,
    };

private:
    enum class EType
    {
        Volume,
        Partition,
    };

    const EType Type;
    const ui64 StartTime;
    const ui64 TabletId = 0;
    const ui32 PartitionIndex = 0;
    const ui32 PartitionCount = 0;

    ui64 Generation = 0;
    TString DiskId;
    TString CachedPrefix;

public:
    TLogTitle(ui64 tabletId, TString diskId, ui64 now);

    TLogTitle(
        ui64 tabletId,
        TString diskId,
        ui64 now,
        ui32 partitionIndex,
        ui32 partitionCount);

    static TString
    GetPartitionPrefix(ui64 tabletId, ui32 partitionIndex, ui32 partitionCount);

    [[nodiscard]] TString Get(EDetails details) const;

    void SetDiskId(TString diskId);
    void SetGeneration(ui32 generation);

private:
    void Rebuild();
    void RebuildForVolume();
    void RebuildForPartition();
    TString GetPartitionPrefix() const;
};

}   // namespace NCloud::NBlockStore::NStorage
