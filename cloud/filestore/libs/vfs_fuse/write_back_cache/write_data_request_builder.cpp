#include "write_data_request_builder.h"

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

struct TRequestData
{
    ui64 Handle = 0;
    TVector<TData> Parts;
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

        // Navigate to the next element if the current one is fully read.
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

////////////////////////////////////////////////////////////////////////////////

class TWriteDataRequestBuilder: public IWriteDataRequestBuilder
{
private:
    const TWriteDataRequestBuilderConfig Config;

public:
    explicit TWriteDataRequestBuilder(
        const TWriteDataRequestBuilderConfig& config)
        : Config(config)
    {
        Y_ABORT_UNLESS(Config.MaxWriteRequestSize > 0);
        Y_ABORT_UNLESS(Config.MaxWriteRequestsCount > 0);
        Y_ABORT_UNLESS(Config.MaxSumWriteRequestsSize > 0);
    }

    TWriteDataBatch BuildWriteDataRequests(
        ui64 nodeId,
        const TDataSupplier& supplier) const override
    {
        auto cachedData = TakeCachedDataWithLimits(supplier);

        if (cachedData.Parts.empty()) {
            return {};
        }

        auto cachedDataParts = BuildDisjointIntervalParts(cachedData.Parts);

        auto requests = DoBuildWriteDataRequests(
            nodeId,
            cachedData.Handle,
            cachedDataParts);

        return {
            .Requests = std::move(requests),
            .AffectedRequestCount = cachedData.Parts.size()};
    }

private:
    TRequestData TakeCachedDataWithLimits(
        const IWriteDataRequestBuilder::TDataSupplier& supplier) const
    {
        NKikimr::TIntervalSet<ui64, 16> intervalSet;
        ui64 estimatedRequestCount = 0;
        ui64 estimatedByteCount = 0;

        TRequestData res;

        auto visitor = [&](ui64 handle, ui64 offset, const TStringBuf& data)
        {
            if (res.Parts.empty()) {
                res.Handle = handle;
            }

            Y_ABORT_UNLESS(!data.Empty(), "Empty requests are not allowed");

            intervalSet.Add(offset, offset + data.size());

            // Value byteCount has to be calculated using O(N) algorithm
            // because TIntervalSet does not track the sum of lengths of
            // intervals. Same for requestCount.
            //
            // A heuristic is used here to reduce complexity:
            // - Pessimistically estimate byteCount and requestCount values
            //   using an assumption that the intervals do not intersect/touch
            // - Calculate the actual values only when the limits are exceeded
            //
            // Notes:
            // - maxWriteRequestsCount is expected to be small (default: 32)
            // - maxSumWriteRequestsSize (default: 16MiB) is expected to be
            //   times larger than maxWriteRequestSize (default: 1MiB) and the
            //   size of TWriteDataEntry buffer (maximal: 1MiB)
            estimatedByteCount += data.size();
            estimatedRequestCount +=
                ((data.size() - 1) / Config.MaxWriteRequestSize) + 1;

            if (estimatedRequestCount > Config.MaxWriteRequestsCount ||
                estimatedByteCount > Config.MaxSumWriteRequestsSize)
            {
                // Calculate the actual values
                ui64 requestCount = 0;
                ui64 byteCount = 0;

                for (const auto& interval: intervalSet) {
                    auto size = interval.second - interval.first;
                    requestCount +=
                        ((size - 1) / Config.MaxWriteRequestSize) + 1;
                    byteCount += size;
                }

                // Still exceed the limits - don't accept more entries
                if (requestCount > Config.MaxWriteRequestsCount ||
                    byteCount > Config.MaxSumWriteRequestsSize)
                {
                    // At least one request should be processed
                    if (!res.Parts.empty()) {
                        return false;
                    }
                }

                estimatedRequestCount = requestCount;
                estimatedByteCount = byteCount;
            }

            res.Parts.push_back({offset, data});
            return true;
        };

        supplier(visitor);

        return res;
    }

    TVector<std::shared_ptr<NProto::TWriteDataRequest>>
    DoBuildWriteDataRequests(
        ui64 nodeId,
        ui64 handle,
        const TVector<TData>& parts) const
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
                request->SetFileSystemId(Config.FileSystemId);
                request->SetNodeId(nodeId);
                request->SetHandle(handle);
                request->SetOffset(reader.GetOffset());

                if (Config.ZeroCopyWriteEnabled) {
                    auto remainingBytes = Config.MaxWriteRequestSize;
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
                    request->SetBuffer(reader.Read(Config.MaxWriteRequestSize));
                }

                res.push_back(std::move(request));
            }

            partIndex = rangeEndIndex;
        }

        return res;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteDataRequestBuilderPtr CreateWriteDataRequestBuilder(
    const TWriteDataRequestBuilderConfig& config)
{
    return std::make_shared<TWriteDataRequestBuilder>(config);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
