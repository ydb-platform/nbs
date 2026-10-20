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

// Prefer a UTC rotation suffix; inspect records only when readRecords is true.
// Unreadable or empty logs have unknown bounds; filesystem mtime is not used.
TProfileLogFile GetProfileLogFile(const TString& path, bool readRecords = false);

// Files must be sorted by EndTime. Use detected record bounds when available;
// dated files use the preceding end as their start, with open first/last bounds.
// Selection uses metadata only, allowing 300 seconds of drift on each side.
// Unknown bounds are unbounded. Since >= until returns an empty list;
// otherwise a single-file list is always retained.
TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until);

}   // namespace NCloud::NFileStore::NProfileTool
