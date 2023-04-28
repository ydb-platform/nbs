#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TManuallyPreemptedVolumeInfo
{
    TInstant LastUpdate;
};

////////////////////////////////////////////////////////////////////////////////

struct TManuallyPreemptedVolumes
{
    THashMap<TString, TManuallyPreemptedVolumeInfo> Volumes;

    void AddVolume(const TString& diskId, TInstant now);
    void RemoveVolume(const TString& diskId);

    std::optional<TManuallyPreemptedVolumeInfo> GetVolume(
        const TString& diskId) const;

    TVector<TString> GetVolumes() const;

    void ClearAll();
    ui32 GetSize() const;

    TString Serialize() const;
    NProto::TError Deserialize(const TString& input);
};

////////////////////////////////////////////////////////////////////////////////

TManuallyPreemptedVolumesPtr CreateManuallyPreemptedVolumes(
    const TString& filePath,
    TLog& log,
    TVector<TString>& criticalEventsStorage);

TManuallyPreemptedVolumesPtr CreateManuallyPreemptedVolumes(
    const TStorageConfigPtr& storageConfig,
    TLog& log,
    TVector<TString>& criticalEventsStorage);

TManuallyPreemptedVolumesPtr CreateManuallyPreemptedVolumes();

}   // namespace NCloud::NBlockStore::NStorage
