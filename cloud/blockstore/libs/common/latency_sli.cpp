#include "latency_sli.h"

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <algorithm>

namespace NCloud::NBlockStore {

bool TLatencySliConfig::Validate()
{
    bool valid = true;
    for (auto& rows: Thresholds) {
        std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) {
            return a.StartBytes < b.StartBytes;
        });
        ui64 end = 0;
        for (const auto& row: rows) {
            if (row.StartBytes < end || row.StartBytes >= row.EndBytes ||
                !row.ThresholdUs)
            {
                rows.clear();
                valid = false;
                break;
            }
            end = row.EndBytes;
        }
    }
    return valid;
}

TString TLatencySliConfig::Serialize() const
{
    if (!Enabled) {
        return {};
    }
    TStringBuilder out;
    out << "1";
    for (size_t write = 0; write < Thresholds.size(); ++write) {
        for (const auto& row: Thresholds[write]) {
            out << ";" << write << ":" << row.StartBytes << ":"
                << row.EndBytes << ":" << row.ThresholdUs;
        }
    }
    return out;
}

std::optional<TLatencySliConfig> TLatencySliConfig::TryParse(const TString& value)
{
    TLatencySliConfig result;
    if (!value) {
        return result;
    }
    TVector<TString> rows;
    StringSplitter(value).Split(';').Collect(&rows);
    if (rows.empty() || rows[0] != "1") {
        return std::nullopt;
    }
    result.Enabled = true;
    for (size_t i = 1; i < rows.size(); ++i) {
        TVector<TString> fields;
        StringSplitter(rows[i]).Split(':').Collect(&fields);
        ui32 write = 0;
        TLatencySliThreshold row;
        if (fields.size() != 4 || !TryFromString(fields[0], write) ||
            write >= result.Thresholds.size() ||
            !TryFromString(fields[1], row.StartBytes) ||
            !TryFromString(fields[2], row.EndBytes) ||
            !TryFromString(fields[3], row.ThresholdUs))
        {
            return std::nullopt;
        }
        result.Thresholds[write].push_back(row);
    }
    if (!result.Validate()) {
        return std::nullopt;
    }
    return result;
}

}   // namespace NCloud::NBlockStore
