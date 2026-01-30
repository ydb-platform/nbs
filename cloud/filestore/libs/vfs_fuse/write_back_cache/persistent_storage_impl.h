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

    NCloud::NProto::TError Init(TPersistentStorageConfig config);

    bool Empty() const override;
    void Visit(const TVisitor& visitor) override;
    ui64 GetMaxSupportedAllocationByteCount() const override;
    const void* Alloc(const TAllocationWriter& writer, size_t size) override;
    void Free(const void* ptr) override;
    TPersistentStorageStats GetStats() const override;

    const TPersistentStorageConfig& GetConfig() const;
};

////////////////////////////////////////////////////////////////////////////////

class TTestStorage: public IPersistentStorage
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TTestStorage();
    ~TTestStorage() override;

    bool Empty() const override;
    void Visit(const TVisitor& visitor) override;
    ui64 GetMaxSupportedAllocationByteCount() const override;
    const void* Alloc(const TAllocationWriter& writer, size_t size) override;
    void Free(const void* ptr) override;
    TPersistentStorageStats GetStats() const override;
    void SetCapacity(size_t capacity);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
