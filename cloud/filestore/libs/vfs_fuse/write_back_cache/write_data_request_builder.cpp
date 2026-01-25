#include "write_data_request_builder_impl.h"

#include "disjoint_interval_builder.h"

#include <contrib/ydb/core/util/interval_set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TData
{
    ui64 Offset = 0;
    TStringBuf Data;
};

////////////////////////////////////////////////////////////////////////////////

// Reads a sequence of contiguous write data entry parts as a single buffer.
// Provides a method to read data across multiple entry parts efficiently.
class TContiguousWriteDataEntryPartsReader
{
public:
    using TIterator = TVector<TData>::const_iterator;

private:
    TIterator Current;
    ui64 CurrentReadOffset = 0;
    ui64 RemainingSize = 0;

public:
    TContiguousWriteDataEntryPartsReader(TIterator begin, TIterator end)
        : Current(begin)
    {
        if (begin < end) {
            Y_DEBUG_ABORT_UNLESS(Validate(begin, end));
            const auto* prev = std::prev(end);
            RemainingSize = prev->Offset + prev->Data.size() - begin->Offset;
        }
    }

    ui64 GetOffset() const
    {
        return Current->Offset + CurrentReadOffset;
    }

    ui64 GetRemainingSize() const
    {
        return RemainingSize;
    }

    TString Read(ui64 maxBytesToRead)
    {
        auto bytesToRead = Min(RemainingSize, maxBytesToRead);
        if (bytesToRead == 0) {
            return {};
        }

        auto buffer = TString::Uninitialized(bytesToRead);

        while (bytesToRead > 0) {
            auto part = ReadInPlace(bytesToRead);
            Y_ABORT_UNLESS(part.size() > 0 && part.size() <= bytesToRead);
            part.copy(buffer.vend() - bytesToRead, part.size());
            bytesToRead -= part.size();
        }

        return buffer;
    }

    TStringBuf ReadInPlace(ui64 maxBytesToRead)
    {
        if (RemainingSize == 0) {
            return {};
        }

        // Nagivate to the next element if the current one is fully read.
        if (CurrentReadOffset == Current->Data.size()) {
            Current++;
            CurrentReadOffset = 0;
            // The next element is guaranteed to be non-empty
            Y_ABORT_UNLESS(!Current->Data.empty());
        }

        const auto len =
            Min(Current->Data.size() - CurrentReadOffset, maxBytesToRead);

        const auto res = Current->Data.SubStr(CurrentReadOffset, len);

        CurrentReadOffset += len;
        RemainingSize -= len;

        return res;
    }

    // Validates a range of data entry parts to ensure they form a contiguous
    // sequence. Additionally checks that each part has non-zero length.
    static bool Validate(TIterator begin, TIterator end)
    {
        if (begin == end) {
            return true;
        }

        for (const auto* it = begin; it != end; it = std::next(it)) {
            if (it->Data.empty()) {
                return false;
            }
        }

        const auto* prev = begin;
        for (const auto* it = std::next(begin); it != end; it = std::next(it)) {
            if (prev->Offset + prev->Data.size() != it->Offset) {
                return false;
            }
            prev = it;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TVector<TData> TakeCachedDataWithLimits(
    const IWriteDataRequestBuilder::TCachedDataSupplier& supplier,
    ui32 maxWriteRequestSize,
    ui32 maxWriteRequestsCount,
    ui32 maxSumWriteRequestsSize)
{
    Y_ABORT_UNLESS(maxWriteRequestSize > 0);
    Y_ABORT_UNLESS(maxWriteRequestsCount > 0);
    Y_ABORT_UNLESS(maxSumWriteRequestsSize > 0);

    NKikimr::TIntervalSet<ui64, 16> intervalSet;
    ui64 estimatedRequestCount = 0;
    ui64 estimatedByteCount = 0;

    TVector<TData> res;

    auto visitor = [&](ui64 offset, TStringBuf data)
    {
        ui64 bufferSize = data.size();
        Y_ABORT_UNLESS(bufferSize > 0);

        intervalSet.Add(offset, offset + bufferSize);

        // Value byteCount has to be calculated using O(N) algorithm
        // because TIntervalSet does not track the sum of lengths of intervals.
        // Same for requestCount.
        //
        // An euristic is used here to reduce complexity:
        // - Pessimistically estimate byteCount and requestCount values using
        //   an assumption that the intervals do not intersect nor touch
        // - Calculate the actual values only when the limits are exceeded
        //
        // Notes:
        // - maxWriteRequestsCount is expected to be small (default: 32)
        // - maxSumWriteRequestsSize (default: 16MiB) is expected to be times
        //   larger than maxWriteRequestSize (default: 1MiB) and the size of
        //   TWriteDataEntry buffer (maximal: 1MiB)
        estimatedRequestCount += (bufferSize - 1) / maxWriteRequestSize + 1;
        estimatedByteCount += bufferSize;

        if (estimatedRequestCount > maxWriteRequestsCount ||
            estimatedByteCount > maxSumWriteRequestsSize)
        {
            // Calculate the actual values
            ui64 requestCount = 0;
            ui64 byteCount = 0;

            for (const auto& interval: intervalSet) {
                auto size = interval.second - interval.first;
                requestCount += (size - 1) / maxWriteRequestSize + 1;
                byteCount += size;
            }

            // Still exceed the limits - don't accept more entries
            if (requestCount > maxWriteRequestsCount ||
                byteCount > maxSumWriteRequestsSize)
            {
                // At least one request should be processed
                if (!res.empty()) {
                    return false;
                }
            }

            estimatedRequestCount = requestCount;
            estimatedByteCount = byteCount;
        }

        res.push_back({offset, data});
        return true;
    };

    supplier(visitor);

    return res;
}

TVector<TData> BuildDisjointIntervalParts(const TVector<TData>& data)
{
    TVector<TInterval> intervals(Reserve(data.size()));

    for (const auto& it: data) {
        intervals.push_back({it.Offset, it.Offset + it.Data.size()});
    }

    auto intervalParts = BuildDisjointIntervalParts(intervals);

    TVector<TData> dataParts;
    for (const auto& part: intervalParts) {
        const auto& src = data[part.Index];
        dataParts.push_back(
            {.Offset = part.Begin,
             .Data = src.Data.SubStr(
                 part.Begin - src.Offset,
                 part.End - part.Begin)});
    }

    return dataParts;
}

TVector<std::shared_ptr<NProto::TWriteDataRequest>> DoBuildWriteDataRequests(
    const TString& fileSystemId,
    ui64 nodeId,
    ui64 handle,
    const TVector<TData>& parts,
    ui32 maxWriteRequestSize,
    bool zeroCopyWriteEnabled)
{
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> res;

    size_t partIndex = 0;
    while (partIndex < parts.size()) {
        auto rangeEndIndex = partIndex;

        while (++rangeEndIndex < parts.size()) {
            const auto& prevPart = parts[rangeEndIndex - 1];
            const auto end = prevPart.Offset + prevPart.Data.size();
            Y_DEBUG_ABORT_UNLESS(end <= parts[rangeEndIndex].Offset);

            if (end != parts[rangeEndIndex].Offset) {
                break;
            }
        }

        TContiguousWriteDataEntryPartsReader reader(
            parts.begin() + partIndex,
            parts.begin() + rangeEndIndex);

        while (reader.GetRemainingSize() > 0) {
            auto request = std::make_shared<NProto::TWriteDataRequest>();
            request->SetFileSystemId(fileSystemId);
            request->SetNodeId(nodeId);
            request->SetHandle(handle);
            request->SetOffset(reader.GetOffset());

            if (zeroCopyWriteEnabled) {
                auto remainingBytes = maxWriteRequestSize;
                while (remainingBytes > 0) {
                    auto part = reader.ReadInPlace(remainingBytes);
                    if (part.empty()) {
                        break;
                    }
                    Y_ABORT_UNLESS(part.size() <= remainingBytes);
                    remainingBytes -= part.size();
                    auto* iovec = request->AddIovecs();
                    iovec->SetBase(reinterpret_cast<ui64>(part.data()));
                    iovec->SetLength(part.size());
                }
            } else {
                request->SetBuffer(reader.Read(maxWriteRequestSize));
            }

            res.push_back(std::move(request));
        }

        partIndex = rangeEndIndex;
    }

    return res;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TWriteDataRequestBuilder::TWriteDataRequestBuilder(
    const TWriteDataRequestBuilderConfig& config)
    : Config(config)
{
    Y_ABORT_UNLESS(Config.MaxWriteRequestSize > 0);
    Y_ABORT_UNLESS(Config.MaxWriteRequestsCount > 0);
    Y_ABORT_UNLESS(Config.MaxSumWriteRequestsSize > 0);
}

TWriteDataBatch TWriteDataRequestBuilder::BuildWriteDataRequests(
    const TString& fileSystemId,
    ui64 nodeId,
    ui64 handle,
    const TCachedDataSupplier& supplier) const
{
    auto cachedData = TakeCachedDataWithLimits(
        supplier,
        Config.MaxWriteRequestSize,
        Config.MaxWriteRequestsCount,
        Config.MaxSumWriteRequestsSize);

    auto cachedDataParts = BuildDisjointIntervalParts(cachedData);

    auto requests = DoBuildWriteDataRequests(
        fileSystemId,
        nodeId,
        handle,
        cachedDataParts,
        Config.MaxWriteRequestSize,
        Config.ZeroCopyWriteEnabled);

    return {
        .Requests = std::move(requests),
        .AffectedRequestCount = cachedData.size()};
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
