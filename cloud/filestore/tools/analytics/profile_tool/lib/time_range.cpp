#include "time_range.h"

#include <util/folder/dirut.h>
#include <util/generic/strbuf.h>
#include <util/system/fstat.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

TMaybe<TInstant> GetProfileLogEndTime(const TString& path)
{
    const auto name = GetBaseName(path);
    // Rotated files are named <log>.YYYY-MM-DDTHH:MM (UTC, rotation time).
    TStringBuf base, suffix;
    TInstant parsedEndTime;
    if (TStringBuf(name).TryRSplit('.', base, suffix) && suffix.size() == 16 &&
        TInstant::TryParseIso8601(TString(suffix) + ":00Z", parsedEndTime))
    {
        return parsedEndTime;
    }
    const TFileStat stat(path);
    if (stat.IsNull()) {
        return Nothing();
    }
    TMaybe<TInstant> endTime;
    if (stat.MTime >= 0) {
        endTime = TInstant::Seconds(stat.MTime) +
                  TDuration::MicroSeconds(stat.MTimeNSec / 1000);
    }
    return endTime;
}

TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until)
{
    if (files.size() <= 1) {
        return files;
    }
    if (since && until && *since >= *until) {
        return {};
    }

    const auto drift = TDuration::Seconds(300);
    size_t selected = 0;
    TMaybe<TInstant> previousEnd;
    for (size_t i = 0; i < files.size(); ++i) {
        // Preserve the original bounds before compacting the vector.
        const auto start = previousEnd;
        previousEnd = files[i].EndTime;
        const auto end = i + 1 < files.size() ? previousEnd : Nothing();
        const bool afterSince = !since || !end || *end + drift >= *since;
        const bool beforeUntil = !until || !start || *start <= *until + drift;
        if (afterSince && beforeUntil) {
            if (selected != i) {
                files[selected] = std::move(files[i]);
            }
            ++selected;
        }
    }
    files.resize(selected);
    return files;
}

}   // namespace NCloud::NFileStore::NProfileTool
