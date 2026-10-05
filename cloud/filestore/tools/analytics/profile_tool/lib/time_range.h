#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NProfileTool {

struct TProfileLogFile
{
    TString Path;
    TMaybe<TInstant> EndTime;
};

// Prefer a .YYYY-MM-DDTHH:MM suffix (UTC); stat the file only as a fallback.
TMaybe<TInstant> GetProfileLogEndTime(const TString& path);

// A single-file list is always returned unchanged.
// Files are supplied in processing order. Each starts at the preceding file's
// end; the first starts 86400 seconds before its end (clamped at the epoch).
// Selection uses metadata only. Unknown bounds are unbounded; reversed
// intervals are retained.
TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until);

}   // namespace NCloud::NFileStore::NProfileTool
