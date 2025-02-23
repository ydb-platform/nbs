#include "write_back_cache.h"

#include <cloud/filestore/libs/service/context.h>

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

    TFileRingBuffer WritesToProcess;

    struct TWriteDataEntry
    {
        ui64 Offset = 0;
        ui32 BufferLength = 0;
        // serialized TWriteDataRequest
        TStringBuf SerializedEntry;
    };

    // maps handle to corresponding entries, used as an index to speed up
    // lookups
    THashMap<ui64, TVector<TWriteDataEntry>> Entries;
    TMutex EntriesLock;

public:
    TImpl(
            IFileStorePtr session,
            const TString& filePath,
            ui32 size)
        : Session(std::move(session))
        , WritesToProcess(filePath, size)
    {
        WritesToProcess.Visit([&] (auto, auto serializedEntry) {
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
        // TODO(svartmetal): optimize, read data only if needed
        return Session->ReadData(std::move(callContext), request).Apply(
            [ptr = weak_from_this(),
            handle = request->GetHandle(),
            readRequestOffset = request->GetOffset(),
            readRequestLength = request->GetLength()] (auto future)
            {
                auto response = future.ExtractValue();

                auto self = ptr.lock();
                if (!self) {
                    return response;
                }

                TVector<TWriteDataEntry> entries;
                with_lock (self->EntriesLock) {
                    entries = self->Entries[handle];
                }

                // TODO(svartmetal): handle error
                Y_ABORT_UNLESS(
                    readRequestLength == response.GetBuffer().length());
                // TODO(svartmetal): support buffer offsetting
                Y_ABORT_UNLESS(0 == response.GetBufferOffset());

                auto* buffer = response.MutableBuffer();

                // TODO(svartmetal): copy only those entries that matter
                for (const auto& entry: entries) {
                    const auto intersectionOffset = Max(
                        readRequestOffset,
                        entry.Offset);
                    const auto intersectionEnd = Min(
                        readRequestOffset + readRequestLength,
                        entry.Offset + entry.BufferLength);

                    if (intersectionOffset < intersectionEnd) {
                        // TODO(svartmetal): avoid parsing with copy
                        NProto::TWriteDataRequest parsedEntry;
                        Y_ABORT_UNLESS(
                            parsedEntry.ParseFromArray(
                                entry.SerializedEntry.data(),
                                entry.SerializedEntry.size()));

                        const char* from = parsedEntry.GetBuffer().data();
                        from += (intersectionOffset - entry.Offset);

                        char* to = buffer->begin();
                        to += (intersectionOffset - readRequestOffset);

                        const auto intersectionLength =
                            intersectionEnd - intersectionOffset;
                        MemCopy(to, from, intersectionLength);
                    }
                }

                return response;
            });
    }

    bool AddWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        // TODO(svartmetal): support buffer offsetting
        Y_ABORT_UNLESS(0 == request->GetBufferOffset());

        // TODO(svartmetal): get rid of double-copy
        TString result;
        Y_ABORT_UNLESS(request->SerializeToString(&result));

        if (!WritesToProcess.Push(result)) {
            return false;
        }

        auto serializedEntry = WritesToProcess.Back();

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
        auto promise = NewPromise();
        promise.SetValue();
        return promise.GetFuture();
    }

    TFuture<void> FlushAllData()
    {
        auto promise = std::make_shared<TPromise<void>>(NewPromise());

        std::function<TFuture<void>(ui32)> flush =
            [=, ptr = weak_from_this(), &flush] (ui32 remaining)
            {
                auto self = ptr.lock();
                if (!self) {
                    return promise->GetFuture();
                }

                if (remaining == 0) {
                    promise->SetValue();
                    return promise->GetFuture();
                }

                const auto serializedEntry = self->WritesToProcess.Front();
                // TODO(svartmetal): avoid parsing with copy
                auto request = std::make_shared<NProto::TWriteDataRequest>();
                Y_ABORT_UNLESS(
                    request->ParseFromArray(
                        serializedEntry.data(),
                        serializedEntry.size()));

                const auto handle = request->GetHandle();

                self->Session->WriteData(
                    MakeIntrusive<TCallContext>(),
                    std::move(request)).Subscribe([=] (auto future) {
                        auto self = ptr.lock();
                        if (!self) {
                            return future;
                        }

                        auto serializedEntry = self->WritesToProcess.Front();
                        with_lock (EntriesLock) {
                            EraseIf(
                                self->Entries[handle],
                                [=] (const auto& x) {
                                    return serializedEntry == x.SerializedEntry;
                                });
                        }

                        self->WritesToProcess.Pop();
                        flush(remaining - 1);
                        return future;
                    });

                return promise->GetFuture();
            };

        return flush(WritesToProcess.Size());
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
