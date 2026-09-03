#include "persistent_storage.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/writer/json.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferStorage: public IPersistentStorage
{
private:
    const IPersistentStorageStatsPtr Stats;
    TFileRingBuffer Storage;
    const TPersistentStorageConfig Config;
    const TLog Log;
    const TString LogTag;

public:
    TFileRingBufferStorage(
        IPersistentStorageStatsPtr stats,
        TPersistentStorageConfig config,
        TLog log,
        TString logTag)
        : Stats(std::move(stats))
        , Storage(
              config.FilePath,
              config.DataCapacity,
              config.MetadataCapacity,
              EFileRingBufferVersion::V6)
        , Config(std::move(config))
        , Log(std::move(log))
        , LogTag(std::move(logTag))
    {
        SetCounters();

        if (Storage.IsCorrupted()) {
            // Reporting corrupted state is handled by TFileRingBuffer
            return;
        }

        NJsonWriter::TBuf json;
        json.BeginObject()
            .WriteKey("FilePath")
            .WriteString(Config.FilePath)
            .WriteKey("Version")
            .WriteULongLong(Storage.GetVersion())
            .WriteKey("RawCapacityByteCount")
            .WriteULongLong(Storage.GetRawCapacity())
            .WriteKey("RawUsedByteCount")
            .WriteULongLong(Storage.GetRawUsedBytesCount())
            .WriteKey("EntryCount")
            .WriteULongLong(Storage.Size())
            .EndObject();

        STORAGE_INFO(
            LogTag << " WriteBackCache storage has been initialized "
                   << json.Str());
    }

    bool Empty() const override
    {
        return Storage.Empty();
    }

    bool IsCorrupted() const override
    {
        return Storage.IsCorrupted();
    }

    NProto::TError Visit(const TVisitor& visitor) override
    {
        auto visitResult = Storage.Visit(
            [&visitor](ui32 checksum, ui32 tag, TStringBuf entry)
            {
                Y_UNUSED(checksum);
                visitor(tag, {entry.data(), entry.size()});
            });

        SetCounters();

        if (HasError(visitResult)) {
            ReportWriteBackCacheCorruptionError(Sprintf(
                "%s Storage::Visit failed with an error: %s",
                LogTag.c_str(),
                FormatError(visitResult).c_str()));
        }

        return visitResult;
    }

    ui64 GetMaxSupportedAllocationByteCount() const override
    {
        return Storage.GetMaxSupportedAllocationByteCount();
    }

    TResultOrError<char*> Alloc(size_t size) override
    {
        auto allocResult = Storage.Alloc(size);

        SetCounters();

        if (HasError(allocResult.Error)) {
            ReportWriteBackCacheCorruptionError(Sprintf(
                "%s Storage::Alloc failed with an error: %s",
                LogTag.c_str(),
                FormatError(allocResult.Error).c_str()));
            return allocResult.Error;
        }

        return allocResult.AllocationPtr;
    }

    NProto::TError Commit(const void* ptr) override
    {
        auto commitResult = Storage.Commit(ptr);

        SetCounters();

        if (HasError(commitResult)) {
            ReportWriteBackCacheCorruptionError(Sprintf(
                "%s Storage::Commit failed with an error: %s",
                LogTag.c_str(),
                FormatError(commitResult).c_str()));
        }

        return commitResult;
    }

    NProto::TError Commit(const void* ptr, ui32 crc32c) override
    {
        auto commitResult = Storage.Commit(ptr, crc32c);

        SetCounters();

        if (HasError(commitResult)) {
            ReportWriteBackCacheCorruptionError(Sprintf(
                "%s Storage::Commit failed with an error: %s",
                LogTag.c_str(),
                FormatError(commitResult).c_str()));
        }

        return commitResult;
    }

    NProto::TError Free(const void* ptr) override
    {
        auto freeResult = Storage.Free(ptr);

        SetCounters();

        if (HasError(freeResult)) {
            ReportWriteBackCacheCorruptionError(Sprintf(
                "%s Storage::Free failed with an error: %s",
                LogTag.c_str(),
                FormatError(freeResult).c_str()));
        }

        return freeResult;
    }

    NProto::TError SetTag(const void* ptr, ui32 tag) override
    {
        auto setTagResult = Storage.SetTag(ptr, tag);

        if (HasError(setTagResult)) {
            ReportWriteBackCacheCorruptionError(Sprintf(
                "%s Storage::SetTag failed with an error: %s",
                LogTag.c_str(),
                FormatError(setTagResult).c_str()));
        }

        return setTagResult;
    }

    void UpdateStats() const override
    {
        Stats->UpdateStats();
    }

private:
    void SetCounters()
    {
        Stats->SetPersistentStorageCounters({
            .RawCapacityBytesCount = Storage.GetRawCapacity(),
            .RawUsedBytesCount = Storage.GetRawUsedBytesCount(),
            .EntryCount = Storage.Size(),
            .MaxObservedEntryByteCount =
                Storage.GetMaxObservedEntryByteCount(),
            .Version = Storage.GetVersion(),
            .IsCorrupted = Storage.IsCorrupted(),
        });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPersistentStoragePtr CreateFileRingBufferPersistentStorage(
    IPersistentStorageStatsPtr stats,
    TPersistentStorageConfig config,
    TLog log,
    TString logTag)
{
    return std::make_shared<TFileRingBufferStorage>(
        std::move(stats),
        std::move(config),
        std::move(log),
        std::move(logTag));
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
