#include "latency_sli.h"

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <algorithm>

namespace NCloud::NBlockStore {

void TLatencySliConfig::Validate()
{
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
                break;
            }
            end = row.EndBytes;
        }
    }
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

TLatencySliConfig TLatencySliConfig::Parse(const TString& value)
{
    TLatencySliConfig result;
    if (!value) {
        return result;
    }
    TVector<TString> rows;
    Split(value, ";", rows);
    Y_ENSURE(rows[0] == "1", "Unsupported latency SLI configuration version");
    result.Enabled = true;
    for (size_t i = 1; i < rows.size(); ++i) {
        TVector<TString> fields;
        Split(rows[i], ":", fields);
        Y_ENSURE(fields.size() == 4, "Invalid latency SLI interval");
        const auto write = FromString<ui32>(fields[0]);
        Y_ENSURE(write < 2, "Invalid latency SLI operation");
        result.Thresholds[write].push_back({
            FromString<ui64>(fields[1]),
            FromString<ui64>(fields[2]),
            FromString<ui64>(fields[3])});
    }
    result.Validate();
    return result;
}

}   // namespace NCloud::NBlockStore
