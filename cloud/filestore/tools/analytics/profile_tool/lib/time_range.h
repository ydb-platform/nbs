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
// Selection allows 300 seconds of drift on both sides of the query interval.
// For the widened valid interval, until < first end selects only the first file, and
// since > last start (the preceding file's end) selects only the last file.
// Files are supplied in processing order. Each starts at the preceding file's
// end; the first has no lower bound.
// Selection uses metadata only. Unknown bounds are unbounded; reversed
// intervals are retained.
TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until);

}   // namespace NCloud::NFileStore::NProfileTool
