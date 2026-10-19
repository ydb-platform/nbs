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

    constexpr auto MaxRotationDrift = TDuration::Seconds(300);
    TVector<TProfileLogFile> result;
    TMaybe<TInstant> fileStart;   // First file is unbounded below.
    for (size_t i = 0; i < files.size(); ++i) {
        const bool isLast = i + 1 == files.size();   // Last is unbounded above.
        const auto fileEnd = isLast ? Nothing() : files[i].EndTime;
        const bool endsAfterSince =
            !since || !fileEnd || *fileEnd + MaxRotationDrift >= *since;
        const bool startsBeforeUntil =
            !until || !fileStart || *fileStart <= *until + MaxRotationDrift;
        fileStart = files[i].EndTime;
        if (endsAfterSince && startsBeforeUntil) {
            result.push_back(std::move(files[i]));
        }
    }
    return result;
}

}   // namespace NCloud::NFileStore::NProfileTool
