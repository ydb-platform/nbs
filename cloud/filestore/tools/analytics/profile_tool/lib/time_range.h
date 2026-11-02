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

// By default, read only the filename's UTC rotation suffix for sorting.
// With readRecords, ignore the suffix and inspect first/last profile records.
// Unreadable or empty logs have unknown bounds; filesystem mtime is not used.
TProfileLogFile GetProfileLogFile(const TString& path, bool readRecords = false);

// Keep single inputs and unbounded queries without probing.
// Otherwise probe record bounds, ignoring filename times and retaining input order.
// Unknown bounds are unbounded.
TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until);

}   // namespace NCloud::NFileStore::NProfileTool
