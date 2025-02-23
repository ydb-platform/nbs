#include "write_back_cache.h"

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/generic/hash.h>
#include <util/generic/mem_copy.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
{
private:
    const IFileStorePtr Session;

    TFileRingBuffer RequestsToProcess;

    struct TWriteDataEntry
    {
        ui64 Offset = 0;
        ui64 BufferLength = 0;
        // serialized TWriteDataRequest
        TStringBuf SerializedEntry;
    };

    THashMap<ui64, TVector<TWriteDataEntry>> Entries;
    TMutex EntriesLock;

public:
    TImpl(
            IFileStorePtr session,
            const TString& filePath,
            ui32 size)
        : Session(std::move(session))
        , RequestsToProcess(filePath, size)
    {
        RequestsToProcess.Visit(
            [&] (
                ui32 checksum,
                TStringBuf serializedEntry)
            {
                Y_UNUSED(checksum);

                NProto::TWriteDataRequest parsedEntry;
                // TODO(svartmetal): avoid parsing with copy
                Y_ABORT_UNLESS(
                    parsedEntry.ParseFromArray(
                        serializedEntry.data(),
                        serializedEntry.size()));

                Entries[parsedEntry.GetHandle()].emplace_back(
                    parsedEntry.GetOffset(),
                    parsedEntry.GetBuffer().length(),
                    serializedEntry);
            });
    }

    TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        return Session->ReadData(callContext, request)
            .Apply(
                [weakSelf = weak_from_this(),
                 handle = request->GetHandle(),
                 readRequestOffset = request->GetOffset(),
                 readRequestLength = request->GetLength()] (auto future)
            {
                auto response = future.ExtractValue();

                auto self = weakSelf.lock();
                if (!self) {
                    return response;
                }

                TVector<TWriteDataEntry> entries;
                with_lock (self->EntriesLock) {
                    entries = self->Entries[handle];
                }

                // TODO(svartmetal): optimize, copy only those entries that
                // matter
                auto* buffer = response.MutableBuffer();
                Y_ABORT_UNLESS(readRequestLength == buffer->length());

                for (const auto& entry: entries) {
                    const auto intersectionLeft = Max(
                        readRequestOffset,
                        entry.Offset);
                    const auto intersectionRight = Min(
                        readRequestOffset + readRequestLength,
                        entry.Offset + entry.BufferLength);

                    if (intersectionLeft < intersectionRight) {
                        // TODO(svartmetal): avoid parsing with copy
                        NProto::TWriteDataRequest parsedEntry;
                        Y_ABORT_UNLESS(
                            parsedEntry.ParseFromArray(
                                entry.SerializedEntry.data(),
                                entry.SerializedEntry.size()));

                        const char* from = parsedEntry.GetBuffer().data();
                        from += (intersectionLeft - entry.Offset);

                        char* to = buffer->begin();
                        to += (intersectionLeft - readRequestOffset);

                        const auto intersectionLength =
                            intersectionRight - intersectionLeft;
                        MemCopy(to, from, intersectionLength);
                    }
                }

                return response;
            });
    }

    bool AddWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        // TODO(svartmetal): get rid of double-copy
        TString result;
        Y_ABORT_UNLESS(request->SerializeToString(&result));

        if (!RequestsToProcess.Push(result)) {
            return false;
        }

        auto serializedEntry = RequestsToProcess.Back();

        with_lock (EntriesLock) {
            Entries[request->GetHandle()].emplace_back(
                request->GetOffset(),
                request->GetBuffer().length(),
                serializedEntry);
        }

        return true;
    }

    TFuture<void> FlushData(ui64 handle)
    {
        Y_UNUSED(handle);

        // TODO(svartmetal): implement me
        auto promise = NewPromise<void>();
        promise.SetValue();
        return promise.GetFuture();
    }

    TFuture<void> FlushAllData()
    {
        // TODO(svartmetal): implement me
        auto promise = NewPromise<void>();
        promise.SetValue();
        return promise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache(
        IFileStorePtr session,
        const TString& filePath,
        ui32 size)
    : Impl(new TImpl(session, filePath, size))
{}

TWriteBackCache::~TWriteBackCache() = default;

TFuture<NProto::TReadDataResponse> TWriteBackCache::ReadData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> request)
{
    return Impl->ReadData(callContext, request);
}

bool TWriteBackCache::AddWriteDataRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    return Impl->AddWriteDataRequest(request);
}

TFuture<void> TWriteBackCache::FlushData(ui64 handle)
{
    return Impl->FlushData(handle);
}

TFuture<void> TWriteBackCache::FlushAllData()
{
    return Impl->FlushAllData();
}

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(
    IFileStorePtr session,
    const TString& filePath,
    ui32 size)
{
    return std::make_unique<TWriteBackCache>(session, filePath, size);
}

}   // namespace NCloud::NFileStore::NFuse
