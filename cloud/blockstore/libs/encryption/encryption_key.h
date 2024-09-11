#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/encryption.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TEncryptionKey
{
private:
    TString Key;

public:
    TEncryptionKey(TString key = {});
    ~TEncryptionKey();

    TEncryptionKey(TEncryptionKey&&) noexcept = default;
    TEncryptionKey& operator=(TEncryptionKey&&) noexcept = default;

    TEncryptionKey(const TEncryptionKey&) = delete;
    TEncryptionKey& operator=(const TEncryptionKey&) = delete;

    const TString& GetKey() const;
    TString GetHash() const;
};

////////////////////////////////////////////////////////////////////////////////

struct IEncryptionKeyProvider
{
    using TResponse = TResultOrError<TEncryptionKey>;

    virtual ~IEncryptionKeyProvider() = default;

    virtual NThreading::TFuture<TResponse> GetKey(
        const NProto::TEncryptionSpec& encryptionSpec,
        const TString& diskId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDefaultEncryptionKeyProvider
{
    virtual ~IDefaultEncryptionKeyProvider() = default;

    virtual auto GetKey(
        const NProto::TEncryptionSpec& encryptionSpec,
        const TString& diskId)
        -> NThreading::TFuture<TResultOrError<TEncryptionKey>> = 0;

    virtual auto GenerateDataEncryptionKey(const TString& diskId)
        -> NThreading::TFuture<TResultOrError<NProto::TKmsKey>> = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IKmsKeyProvider
{
    using TResponse = TResultOrError<TEncryptionKey>;

    virtual ~IKmsKeyProvider() = default;

    virtual NThreading::TFuture<TResponse> GetKey(
        const NProto::TKmsKey& kmsKey,
        const TString& diskId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IRootKmsKeyProvider
{
    virtual ~IRootKmsKeyProvider() = default;

    virtual auto GetKey(const NProto::TKmsKey& kmsKey, const TString& diskId)
        -> NThreading::TFuture<TResultOrError<TEncryptionKey>> = 0;

    virtual auto GenerateDataEncryptionKey(const TString& diskId)
        -> NThreading::TFuture<TResultOrError<NProto::TKmsKey>> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKmsKeyProviderPtr CreateKmsKeyProviderStub();
IRootKmsKeyProviderPtr CreateRootKmsKeyProviderStub();

IEncryptionKeyProviderPtr CreateEncryptionKeyProvider(
    IKmsKeyProviderPtr kmsKeyProvider);

IDefaultEncryptionKeyProviderPtr CreateDefaultEncryptionKeyProvider(
    IRootKmsKeyProviderPtr rootKmsKeyProvider);

}   // namespace NCloud::NBlockStore
