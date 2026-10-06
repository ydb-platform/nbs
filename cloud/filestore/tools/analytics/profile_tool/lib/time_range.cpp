#include "time_range.h"

#include <util/folder/dirut.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>

namespace NCloud::NFileStore::NProfileTool {

TMaybe<TInstant> GetProfileLogEndTime(const TString& path)
{
    const auto name = GetBaseName(path);
    constexpr size_t SuffixLength = 17;   // .YYYY-MM-DDTHH:MM
    if (name.size() >= SuffixLength && name[name.size() - SuffixLength] == '.')
    {
        const auto timestamp = name.substr(name.size() - SuffixLength + 1);
        TInstant endTime;
        if (TInstant::TryParseIso8601(timestamp + ":00Z", endTime)) {
            return endTime;
        }
    }
    const TFileStat stat(path);
    if (stat.IsNull()) {
        ythrow TFileError() << "Failed to stat profile log " << path;
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
    // Widen only file selection; per-request time filters remain exact.
    const auto drift = TDuration::Seconds(300);
    const auto selectionSince =
        since ? TMaybe<TInstant>(*since - drift) : Nothing();
    const auto selectionUntil =
        until ? TMaybe<TInstant>(*until + drift) : Nothing();
    if (selectionSince && selectionUntil && *selectionSince >= *selectionUntil) {
        return {};
    }
    if (selectionUntil && files.front().EndTime &&
        *selectionUntil < *files.front().EndTime)
    {
        files.resize(1);
        return files;
    }
    const auto& lastStart = files[files.size() - 2].EndTime;
    if (selectionSince && lastStart && *selectionSince > *lastStart) {
        files.front() = std::move(files.back());
        files.resize(1);
        return files;
    }

    size_t selected = 0;
    TMaybe<TInstant> previousEnd;
    for (size_t i = 0; i < files.size(); ++i) {
        const auto& file = files[i];
        auto start = previousEnd;
        if (i == 0 && file.EndTime) {
            const auto day = TDuration::Seconds(86400);
            start = *file.EndTime >= TInstant::Seconds(86400)
                        ? *file.EndTime - day
                        : TInstant::Zero();
        }
        // Update even for excluded files: the chain precedes interval
        // filtering.
        previousEnd = file.EndTime;
        const bool reversed = start && file.EndTime && *start > *file.EndTime;
        if (reversed ||
            ((!selectionSince || !file.EndTime ||
              *file.EndTime >= *selectionSince) &&
             (!selectionUntil || !start || *start <= *selectionUntil)))
        {
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
