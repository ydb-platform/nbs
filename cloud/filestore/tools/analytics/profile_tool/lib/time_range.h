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
};

// Prefer a .YYYY-MM-DDTHH:MM suffix (UTC); stat the file only as a fallback.
// Return Nothing() if stat fails or the modification time is before the epoch.
TMaybe<TInstant> GetProfileLogEndTime(const TString& path);

// Files must be sorted by EndTime. Each starts at the preceding file's end;
// the first is unbounded below and the last is unbounded above.
// Selection uses metadata only, allowing 300 seconds of drift on each side.
// Unknown bounds are unbounded. Since >= until returns an empty list;
// otherwise a single-file list is always retained.
TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until);

}   // namespace NCloud::NFileStore::NProfileTool
