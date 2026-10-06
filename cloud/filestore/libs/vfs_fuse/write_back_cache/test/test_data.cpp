#include "test_data.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/random/random.h>
#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <optional>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

TStringBuf AsStringBuf(const NProto::TIovec& iovec)
{
    return {reinterpret_cast<const char*>(iovec.GetBase()), iovec.GetLength()};
}

TMemoryOutput AsMemoryOutput(const NProto::TIovec& iovec)
{
    return {reinterpret_cast<char*>(iovec.GetBase()), iovec.GetLength()};
}

TStringBuf ReadBytes(const TString& data, ui64 offset, ui64 length)
{
    auto result = TStringBuf(data);
    result.Skip(Min<ui64>(offset, result.size()));
    result.Trunc(Min<ui64>(length, result.size()));
    return result;
}

void WriteBytes(TString& data, ui64 offset, TStringBuf buffer)
{
    const auto newSize = Max<ui64>(data.size(), offset + buffer.size());
    data.resize(newSize, 0);
    data.replace(offset, buffer.size(), buffer);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString ExtractWriteData(const NProto::TWriteDataRequest& request)
{
    if (request.GetIovecs().empty()) {
        return TString(TStringBuf(request.GetBuffer())
                           .Skip(
                               Min<size_t>(
                                   request.GetBufferOffset(),
                                   request.GetBuffer().size())));
    }

    TString data;
    for (const auto& iovec: request.GetIovecs()) {
        data.append(AsStringBuf(iovec));
    }
    return data;
}

////////////////////////////////////////////////////////////////////////////////

class TTestData::TImpl
{
private:
    struct TOperationLog
    {
        TString Tag;
        TLog Log;

        TOperationLog(TString tag, const TLog& log)
            : Tag(std::move(tag))
            , Log(log)
        {}
    };

    struct TNodeData
    {
        TAdaptiveLock Lock;
        TString Data;
    };

    TAdaptiveLock NodesLock;
    THashMap<ui64, std::unique_ptr<TNodeData>> Nodes;
    std::atomic<ui64> BytesWritten = 0;
    std::atomic<ui64> NextLogSequenceId = 1;
    std::optional<TOperationLog> ReadLog;
    std::optional<TOperationLog> WriteLog;

public:
    TImpl() = default;

    void EnableLogReads(TString logTag, const TLog& log)
    {
        ReadLog.emplace(std::move(logTag), log);
    }

    void EnableLogWrites(TString logTag, const TLog& log)
    {
        WriteLog.emplace(std::move(logTag), log);
    }

    NProto::TReadDataResponse Read(
        const NProto::TReadDataRequest& request,
        ui32 responseBufferOffsetLimit)
    {
        NProto::TReadDataResponse response;
        auto data =
            Read(request.GetNodeId(), request.GetOffset(), request.GetLength());

        if (request.GetIovecs().empty()) {
            const auto responseBufferOffset =
                responseBufferOffsetLimit
                    ? RandomNumber(responseBufferOffsetLimit)
                    : 0;
            TString buffer(responseBufferOffset + data.size(), 0);
            data.copy(buffer.begin() + responseBufferOffset, data.size());

            response.SetBuffer(std::move(buffer));
            response.SetBufferOffset(responseBufferOffset);
            return response;
        }

        response.SetLength(data.size());
        auto remainingData = TStringBuf(data);
        for (const auto& iovec: request.GetIovecs()) {
            if (remainingData.empty()) {
                break;
            }

            auto output = AsMemoryOutput(iovec);
            const auto length = Min(remainingData.size(), output.Avail());
            output.Write(remainingData.Head(length));
            remainingData.Skip(length);
        }

        return response;
    }

    TString Read(ui64 nodeId, ui64 offset, ui64 length)
    {
        ui64 sequenceId = 0;
        auto* nodeData = FindNodeDataForRead(nodeId, sequenceId);
        if (!nodeData) {
            LogRead(sequenceId, nodeId, offset, {});
            return {};
        }

        TString data;
        {
            auto guard = Guard(nodeData->Lock);
            data = ReadBytes(nodeData->Data, offset, length);
            sequenceId = GetNextReadLogSequenceId();
        }

        LogRead(sequenceId, nodeId, offset, data);
        return data;
    }

    TString Write(const NProto::TWriteDataRequest& request)
    {
        auto data = ExtractWriteData(request);
        Write(request.GetNodeId(), request.GetOffset(), data);
        return data;
    }

    void Write(ui64 nodeId, ui64 offset, TStringBuf data)
    {
        ui64 sequenceId = 0;
        {
            auto* nodeData = GetOrCreateNodeData(nodeId);
            auto guard = Guard(nodeData->Lock);
            WriteBytes(nodeData->Data, offset, data);
            BytesWritten.fetch_add(data.size());
            sequenceId = GetNextWriteLogSequenceId();
        }

        LogWrite(sequenceId, nodeId, offset, data);
    }

    bool Contains(ui64 nodeId)
    {
        auto guard = Guard(NodesLock);
        return Nodes.contains(nodeId);
    }

    TString ReadAll(ui64 nodeId)
    {
        auto* nodeData = FindNodeData(nodeId);
        if (!nodeData) {
            return {};
        }

        auto guard = Guard(nodeData->Lock);
        return nodeData->Data;
    }

    TVector<ui64> GetNodeIds()
    {
        auto guard = Guard(NodesLock);

        TVector<ui64> nodeIds;
        nodeIds.reserve(Nodes.size());
        for (const auto& [nodeId, _]: Nodes) {
            nodeIds.push_back(nodeId);
        }
        return nodeIds;
    }

    TString Dump()
    {
        auto nodeIds = GetNodeIds();
        Sort(nodeIds);

        TStringBuilder result;
        for (const auto nodeId: nodeIds) {
            auto* nodeData = FindNodeData(nodeId);
            Y_ABORT_UNLESS(nodeData);

            auto guard = Guard(nodeData->Lock);
            result << "(" << nodeId << ":" << nodeData->Data.size() << ":"
                   << nodeData->Data << ")";
        }
        return result;
    }

    ui64 GetBytesWritten()
    {
        return BytesWritten.load();
    }

private:
    ui64 GetNextReadLogSequenceId()
    {
        return ReadLog ? NextLogSequenceId.fetch_add(1) : 0;
    }

    ui64 GetNextWriteLogSequenceId()
    {
        return WriteLog ? NextLogSequenceId.fetch_add(1) : 0;
    }

    void LogRead(ui64 sequenceId, ui64 nodeId, ui64 offset, TStringBuf data)
    {
        if (ReadLog) {
            const auto& Log = ReadLog->Log;
            STORAGE_INFO(
                ReadLog->Tag << "[sequenceId=" << sequenceId << "] Read "
                             << TString(data).Quote() << " from @" << nodeId
                             << " at offset " << offset);
        }
    }

    void LogWrite(ui64 sequenceId, ui64 nodeId, ui64 offset, TStringBuf data)
    {
        if (WriteLog) {
            const auto& Log = WriteLog->Log;
            STORAGE_INFO(
                WriteLog->Tag << "[sequenceId=" << sequenceId << "] Written "
                              << TString(data).Quote() << " to @" << nodeId
                              << " at offset " << offset);
        }
    }

    TNodeData* GetOrCreateNodeData(ui64 nodeId)
    {
        auto guard = Guard(NodesLock);
        auto& nodeData = Nodes[nodeId];
        if (!nodeData) {
            nodeData = std::make_unique<TNodeData>();
        }
        return nodeData.get();
    }

    TNodeData* FindNodeData(ui64 nodeId)
    {
        auto guard = Guard(NodesLock);
        const auto it = Nodes.find(nodeId);
        return it != Nodes.end() ? it->second.get() : nullptr;
    }

    TNodeData* FindNodeDataForRead(ui64 nodeId, ui64& sequenceId)
    {
        auto guard = Guard(NodesLock);
        const auto it = Nodes.find(nodeId);
        if (it == Nodes.end()) {
            sequenceId = GetNextReadLogSequenceId();
            return nullptr;
        }
        return it->second.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTestData::TTestData()
    : Impl(std::make_unique<TImpl>())
{}

TTestData::~TTestData() = default;

void TTestData::EnableLogReads(TString logTag, const TLog& log)
{
    Impl->EnableLogReads(std::move(logTag), log);
}

void TTestData::EnableLogWrites(TString logTag, const TLog& log)
{
    Impl->EnableLogWrites(std::move(logTag), log);
}

NProto::TReadDataResponse TTestData::Read(
    const NProto::TReadDataRequest& request,
    ui32 responseBufferOffsetLimit) const
{
    return Impl->Read(request, responseBufferOffsetLimit);
}

TString TTestData::Read(ui64 nodeId, ui64 offset, ui64 length) const
{
    return Impl->Read(nodeId, offset, length);
}

TString TTestData::Write(const NProto::TWriteDataRequest& request)
{
    return Impl->Write(request);
}

void TTestData::Write(ui64 nodeId, ui64 offset, TStringBuf data)
{
    Impl->Write(nodeId, offset, data);
}

bool TTestData::Contains(ui64 nodeId) const
{
    return Impl->Contains(nodeId);
}

TString TTestData::ReadAll(ui64 nodeId) const
{
    return Impl->ReadAll(nodeId);
}

TVector<ui64> TTestData::GetNodeIds() const
{
    return Impl->GetNodeIds();
}

TString TTestData::Dump() const
{
    return Impl->Dump();
}

ui64 TTestData::GetBytesWritten() const
{
    return Impl->GetBytesWritten();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
