#pragma once

#include "persistent_storage.h"

#include <cloud/storage/core/protos/error.pb.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageConfig
{
    TString FilePath;
    ui64 DataCapacity = 0;
    ui64 MetadataCapacity = 0;
    bool EnableChecksumCalculation = false;
    bool EnableChecksumValidation = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageStats
{
    ui64 RawCapacityByteCount = 0;
    ui64 RawUsedByteCount = 0;
    ui64 EntryCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferStorage: public IPersistentStorage
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TFileRingBufferStorage();
    TFileRingBufferStorage(TFileRingBufferStorage&&) noexcept;
    TFileRingBufferStorage& operator=(TFileRingBufferStorage&&) noexcept;
    ~TFileRingBufferStorage() override;

    NProto::TError Init(TPersistentStorageConfig config);

    bool Empty() const override;
    void Visit(const TVisitor& visitor) override;
    ui64 GetMaxSupportedAllocationByteCount() const override;
    const void* Alloc(const TAllocationWriter& writer, size_t size) override;
    void Free(const void* ptr) override;

    const TPersistentStorageConfig& GetConfig() const;
    TPersistentStorageStats GetStats() const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
