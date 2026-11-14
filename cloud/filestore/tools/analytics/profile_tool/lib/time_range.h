#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

struct TProfileLogFile
{
    TString Path;
    TMaybe<TInstant> StartTime;
    TMaybe<TInstant> EndTime;
};

// Leave bounds unknown unless readRecords requests boundary-frame probing.
// Use the eventlog library to scan from the start and in expanding tail windows
// (initially 64 KiB). Stop once usable boundary frames are found; unusable
// boundaries may require scanning the whole file. No frame index is built.
// Inspect all records in first/last usable frames; filenames do not supply
// times. Timestamps before 2000-01-01 UTC are excluded from estimates.
// Unreadable logs or logs without valid timestamps have unknown bounds.
// Filesystem mtime is not used.
TProfileLogFile GetProfileLogFile(
    const TString& path,
    bool readRecords = false);

// Keep single inputs and unbounded queries.
// Otherwise select using previously detected bounds, retaining input order.
// Selection does not open files.
// Unknown bounds are unbounded.
TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until);

}   // namespace NCloud::NFileStore::NProfileTool
