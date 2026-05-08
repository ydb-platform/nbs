#include "persistent_storage.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_ring_buffer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/writer/json.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

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
        , Storage(config.FilePath, config.DataCapacity, config.MetadataCapacity)
        , Config(std::move(config))
        , Log(std::move(log))
        , LogTag(std::move(logTag))
    {
        SetCounters();
    }

    NProto::TError Init()
    {
        if (Storage.IsCorrupted()) {
            return MakeError(E_FAIL, "Data structure is corrupted");
        }

        if (!Storage.ValidateMetadata()) {
            Storage.SetCorrupted();
            SetCounters();
            return MakeError(E_FAIL, "Metadata is corrupted");
        }

        if (Config.EnableChecksumValidation && !Storage.Validate().empty()) {
            Storage.SetCorrupted();
            SetCounters();
            return MakeError(E_FAIL, "Data entries are corrupted");
        }

        NJsonWriter::TBuf json;
        json.BeginObject()
            .WriteKey("FilePath")
            .WriteString(Config.FilePath)
            .WriteKey("RawCapacityByteCount")
            .WriteULongLong(Storage.GetRawCapacity())
            .WriteKey("RawUsedByteCount")
            .WriteULongLong(Storage.GetRawUsedBytesCount())
            .WriteKey("EntryCount")
            .WriteULongLong(Storage.Size())
            .EndObject();

        STORAGE_INFO(
            LogTag << " WriteBackCache has been initialized " << json.Str());

        return {};
    }

    bool Empty() const override
    {
        return Storage.Empty();
    }

    void Visit(const TVisitor& visitor) override
    {
        Storage.Visit(
            [&visitor](ui32 checksum, TStringBuf entry)
            {
                Y_UNUSED(checksum);
                visitor({entry.data(), entry.size()});
            });

        SetCounters();
    }

    ui64 GetMaxSupportedAllocationByteCount() const override
    {
        return Storage.GetMaxSupportedAllocationByteCount();
    }

    TResultOrError<char*> Alloc(size_t size) override
    {
        return Storage.Alloc(size);
    }

    void Commit() override
    {
        bool success = Storage.Commit();
        Y_ENSURE(success, "Failed to commit allocation");
        SetCounters();
    }

    void Free(const void* ptr) override
    {
        bool success = Storage.Free(ptr);
        Y_ENSURE(success, "Failed to free pointer " << ptr);
        SetCounters();
    }

    void UpdateStats() const override
    {
        Stats->UpdateStats();
    }

private:
    void SetCounters()
    {
        Stats->SetPersistentStorageCounters(
            /* rawCapacityBytesCount = */ Storage.GetRawCapacity(),
            /* rawUsedBytesCount = */ Storage.GetRawUsedBytesCount(),
            /* entryCount = */ Storage.Size(),
            /* isCorrupted = */ Storage.IsCorrupted());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IPersistentStoragePtr> CreateFileRingBufferPersistentStorage(
    IPersistentStorageStatsPtr stats,
    TPersistentStorageConfig config,
    TLog log,
    TString logTag)
{
    auto storage = std::make_shared<TFileRingBufferStorage>(
        std::move(stats),
        std::move(config),
        std::move(log),
        std::move(logTag));

    auto error = storage->Init();
    if (HasError(error)) {
        return error;
    }

    return static_cast<IPersistentStoragePtr>(storage);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
