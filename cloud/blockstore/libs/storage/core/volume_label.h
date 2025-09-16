#pragma once

#include <util/generic/string.h>

#include <tuple>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString DiskIdToPathDeprecated(const TString& diskId);

TString DiskIdToPath(const TString& diskId);

TString PathNameToDiskId(const TString& pathName);

// Converts diskId relative to rootDir to a pair of (dir, name) for the volume.
std::tuple<TString, TString> DiskIdToVolumeDirAndNameDeprecated(
    const TString& rootDir,
    const TString& diskId);

// Converts diskId relative to rootDir to a pair of (dir, name) for the volume.
std::tuple<TString, TString> DiskIdToVolumeDirAndName(
    const TString& rootDir,
    const TString& diskId);

// Adds a suffix to the disk name so that the disk and its copy always differ in
// the same way.
TString GetSecondaryDiskId(const TString& diskId);

// Remove suffix of secondary disk if needed.
TString GetLogicalDiskId(const TString& diskId);

// Returns whether the diskId has a secondary disk suffix.
bool IsSecondaryDiskId(const TString& diskId);

}   // namespace NCloud::NBlockStore::NStorage
