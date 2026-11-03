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
    TMaybe<TInstant> EndTime;
    TMaybe<TInstant> StartTime = {};
};

// Leave bounds unknown unless readRecords requests boundary-frame probing.
// Inspect all records in first/last usable frames; filenames do not supply
// times. Unreadable or empty logs have unknown bounds; filesystem mtime is not
// used.
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
