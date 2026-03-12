#include "write_data_request_builder.h"

#include "disjoint_interval_builder.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

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

class TWriteRequestCounter
{
private:
    const ui64 MaxWriteRequestSize;
    TDisjointIntervalMap<ui64, ui64> SeparatedIntervalsMap;
    ui64 WriteRequestCount = 0;
    ui64 SumWriteRequestsSize = 0;

public:
    explicit TWriteRequestCounter(ui64 maxSumWriteRequestSize)
        : MaxWriteRequestSize(maxSumWriteRequestSize)
    {}

    void AddInterval(ui64 begin, ui64 end)
    {
        Y_ABORT_UNLESS(begin < end);

        // Remove all overlapping and touching intervals
        SeparatedIntervalsMap.VisitOverlapping(
            begin > 0 ? begin - 1 : 0,
            end < Max<ui64>() ? end + 1 : Max<ui64>(),
            [this, &begin, &end](auto it)
            {
                const TDisjointIntervalMap<ui64, ui64>::TItem& e = it->second;
                WriteRequestCount -= e.Value;
                SumWriteRequestsSize -= e.End - e.Begin;
                begin = Min(begin, e.Begin);
                end = Max(end, e.End);
                SeparatedIntervalsMap.Remove(it);
            });

        const ui64 count = ((end - begin - 1) / MaxWriteRequestSize) + 1;
        SeparatedIntervalsMap.Add(begin, end, count);
        WriteRequestCount += count;
        SumWriteRequestsSize += end - begin;
    }

    ui64 GetWriteRequestCount() const
    {
        return WriteRequestCount;
    }

    ui64 GetSumWriteRequestsSize() const
    {
        return SumWriteRequestsSize;
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

    TWriteRequestCounter WriteDataRequestCounter;

    ui64 Handle = 0;
    TVector<TWriteDataRequestPart> InputRequests;

public:
    TWriteDataRequestBatchBuilder(
        ui64 nodeId,
        TWriteDataRequestBuilderConfig config)
        : NodeId(nodeId)
        , Config(std::move(config))
        , WriteDataRequestCounter(Config.MaxWriteRequestSize)
    {}

    bool AddRequest(ui64 handle, ui64 offset, TStringBuf data) override
    {
        Y_ABORT_UNLESS(!data.empty(), "Empty requests are not allowed");

        if (InputRequests.empty()) {
            Handle = handle;
        }

        WriteDataRequestCounter.AddInterval(offset, offset + data.size());

        if ((WriteDataRequestCounter.GetWriteRequestCount() >
                 Config.MaxWriteRequestsCount ||
             WriteDataRequestCounter.GetSumWriteRequestsSize() >
                 Config.MaxSumWriteRequestsSize) &&
            !InputRequests.empty())
        {
            return false;
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
                Y_ABORT_UNLESS(end <= dataParts[rangeEndIndex].Offset);

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
