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
    if (files.size() == 1) {
        return files;
    }
    if (since && until && *since >= *until) {
        return {};
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
        if (reversed || ((!since || !file.EndTime || *file.EndTime >= *since) &&
                         (!until || !start || *start < *until)))
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
