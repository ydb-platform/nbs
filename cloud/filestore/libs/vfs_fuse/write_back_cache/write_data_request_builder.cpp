#include "write_data_request_builder.h"

#include "disjoint_interval_builder.h"

#include <contrib/ydb/core/util/interval_set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestPart
{
    // Offset in the node
    ui64 Offset = 0;
    // Data to be written at the specified offset
    TStringBuf Data;
};

////////////////////////////////////////////////////////////////////////////////

// Reads a sequence of contiguous write data entry parts as a single buffer.
// Provides a method to read data across multiple entry parts efficiently.
class TContiguousWriteDataRequestPartsReader
{
public:
    using TWriteDataRequestPartIterator =
        TVector<TWriteDataRequestPart>::const_iterator;

private:
    TWriteDataRequestPartIterator Current;
    ui64 CurrentReadOffset = 0;
    ui64 RemainingSize = 0;

public:
    TContiguousWriteDataRequestPartsReader(
        TWriteDataRequestPartIterator begin,
        TWriteDataRequestPartIterator end)
        : Current(begin)
    {
        Y_ABORT_UNLESS(begin < end);
        Y_DEBUG_ABORT_UNLESS(Validate(begin, end));
        const auto* prev = std::prev(end);
        RemainingSize = prev->Offset + prev->Data.size() - begin->Offset;
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
    static bool Validate(
        TWriteDataRequestPartIterator begin,
        TWriteDataRequestPartIterator end)
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

TVector<TWriteDataRequestPart> BuildDisjointRequestDataParts(
    const TVector<TWriteDataRequestPart>& data)
{
    TVector<TInterval> intervals(Reserve(data.size()));

    for (const auto& it: data) {
        intervals.push_back({it.Offset, it.Offset + it.Data.size()});
    }

    auto intervalParts = BuildDisjointIntervalParts(intervals);

    TVector<TWriteDataRequestPart> dataParts;
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

class TWriteDataRequestBatchBuilder: public IWriteDataRequestBatchBuilder
{
private:
    const ui64 NodeId;
    const TWriteDataRequestBuilderConfig Config;

    NKikimr::TIntervalSet<ui64, 16> IntervalSet;
    ui64 EstimatedRequestCount = 0;
    ui64 EstimatedByteCount = 0;

    ui64 Handle = 0;
    TVector<TWriteDataRequestPart> InputRequests;

public:
    TWriteDataRequestBatchBuilder(
        ui64 nodeId,
        TWriteDataRequestBuilderConfig config)
        : NodeId(nodeId)
        , Config(std::move(config))
    {}

    bool AddRequest(ui64 handle, ui64 offset, TStringBuf data) override
    {
        Y_ABORT_UNLESS(!data.empty(), "Empty requests are not allowed");

        if (InputRequests.empty()) {
            Handle = handle;
        }

        IntervalSet.Add(offset, offset + data.size());

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
        EstimatedRequestCount +=
            ((data.size() - 1) / Config.MaxWriteRequestSize) + 1;
        EstimatedByteCount += data.size();

        if (EstimatedRequestCount > Config.MaxWriteRequestsCount ||
            EstimatedByteCount > Config.MaxSumWriteRequestsSize)
        {
            // Calculate the actual values
            ui64 requestCount = 0;
            ui64 byteCount = 0;

            for (const auto& interval: IntervalSet) {
                auto size = interval.second - interval.first;
                requestCount += ((size - 1) / Config.MaxWriteRequestSize) + 1;
                byteCount += size;
            }

            // Still exceed the limits - don't accept more entries
            if ((requestCount > Config.MaxWriteRequestsCount ||
                 byteCount > Config.MaxSumWriteRequestsSize) &&
                !InputRequests.empty())
            {
                return false;
            }

            EstimatedRequestCount = requestCount;
            EstimatedByteCount = byteCount;
        }

        InputRequests.push_back({offset, data});
        return true;
    }

    TWriteDataRequestBatch Build() override
    {
        if (InputRequests.empty()) {
            return {};
        }

        auto dataParts = BuildDisjointRequestDataParts(InputRequests);

        TWriteDataRequestBatch res;
        res.Requests = BuildRequestsFromParts(dataParts);
        res.AffectedRequestCount = InputRequests.size();

        return res;
    }

private:
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> BuildRequestsFromParts(
        const TVector<TWriteDataRequestPart>& dataParts)
    {
        TVector<std::shared_ptr<NProto::TWriteDataRequest>> res;

        size_t partIndex = 0;
        while (partIndex < dataParts.size()) {
            auto rangeEndIndex = partIndex;

            while (++rangeEndIndex < dataParts.size()) {
                const auto& prevPart = dataParts[rangeEndIndex - 1];
                const auto end = prevPart.Offset + prevPart.Data.size();
                Y_DEBUG_ABORT_UNLESS(end <= dataParts[rangeEndIndex].Offset);

                if (end != dataParts[rangeEndIndex].Offset) {
                    break;
                }
            }

            TContiguousWriteDataRequestPartsReader reader(
                dataParts.begin() + partIndex,
                dataParts.begin() + rangeEndIndex);

            while (reader.GetRemainingSize() > 0) {
                auto request = std::make_shared<NProto::TWriteDataRequest>();
                request->SetFileSystemId(Config.FileSystemId);
                request->SetNodeId(NodeId);
                request->SetHandle(Handle);
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

////////////////////////////////////////////////////////////////////////////////

class TWriteDataRequestBuilder: public IWriteDataRequestBuilder
{
private:
    TWriteDataRequestBuilderConfig Config;

public:
    explicit TWriteDataRequestBuilder(
        const TWriteDataRequestBuilderConfig& config)
        : Config(config)
    {}

    std::unique_ptr<IWriteDataRequestBatchBuilder>
    CreateWriteDataRequestBatchBuilder(ui64 nodeId) override
    {
        return std::make_unique<TWriteDataRequestBatchBuilder>(nodeId, Config);
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
