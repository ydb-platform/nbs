#include "latency_thresholds_config.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

TLatencyThresholdLadder ParseLatencyThresholdsConfigV1(TStringBuf value)
{
    if (value == "unconfigured") {
        return {};
    }

    Y_ENSURE(value, "latency thresholds v1 config is empty");

    TLatencyThresholdLadder result;
    size_t offset = 0;
    while (offset < value.size()) {
        const size_t comma = value.find(',', offset);
        const TStringBuf item = value.SubStr(
            offset,
            comma == TStringBuf::npos ? TStringBuf::npos : comma - offset);

        const size_t firstColon = item.find(':');
        const size_t secondColon = firstColon == TStringBuf::npos
            ? TStringBuf::npos
            : item.find(':', firstColon + 1);
        Y_ENSURE(
            item && firstColon != TStringBuf::npos &&
                secondColon != TStringBuf::npos &&
                item.find(':', secondColon + 1) == TStringBuf::npos,
            "invalid latency thresholds v1 bucket: " << item);

        const ui64 minRequestBytes =
            FromString<ui64>(item.SubStr(0, firstColon));
        const ui32 readThresholdMs = FromString<ui32>(
            item.SubStr(firstColon + 1, secondColon - firstColon - 1));
        const ui32 writeThresholdMs =
            FromString<ui32>(item.SubStr(secondColon + 1));

        Y_ENSURE(
            readThresholdMs && writeThresholdMs,
            "latency thresholds v1 values must be positive");
        Y_ENSURE(
            !result.empty() || minRequestBytes == 0,
            "latency thresholds v1 first bucket must start at zero");
        Y_ENSURE(
            result.empty() ||
                result.back().MinRequestBytes < minRequestBytes,
            "latency thresholds v1 bucket bounds must be strictly increasing");
        Y_ENSURE(
            result.size() < MaxLatencyThresholdBucketsPerMediaKind,
            "latency thresholds v1 config has too many buckets");

        result.push_back(TLatencyThresholdBucket{
            .MinRequestBytes = minRequestBytes,
            .ReadThreshold = TDuration::MilliSeconds(readThresholdMs),
            .WriteThreshold = TDuration::MilliSeconds(writeThresholdMs),
        });

        if (comma == TStringBuf::npos) {
            break;
        }
        offset = comma + 1;
        Y_ENSURE(
            offset < value.size(),
            "latency thresholds v1 config has empty bucket");
    }

    return result;
}

TString SerializeLatencyThresholdsConfigV1(const TLatencyThresholdLadder* ladder)
{
    if (!ladder) {
        return "unconfigured";
    }

    TStringBuilder result;
    bool first = true;
    for (const auto& bucket: *ladder) {
        if (!first) {
            result << ',';
        }
        first = false;
        result << bucket.MinRequestBytes << ':'
               << bucket.ReadThreshold.MilliSeconds() << ':'
               << bucket.WriteThreshold.MilliSeconds();
    }
    return result;
}

}   // namespace NCloud::NBlockStore::NVHostServer
