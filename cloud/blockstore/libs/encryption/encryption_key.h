#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/encryption.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

#include <openssl/sha.h>

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
    virtual ~IEncryptionKeyProvider() = default;

    virtual TResultOrError<TEncryptionKey> GetKey(
        const NProto::TEncryptionSpec& encryptionSpec) = 0;

    virtual TResultOrError<TString> GetKeyHash(
        const NProto::TEncryptionSpec& encryptionSpec) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IEncryptionKeyProviderPtr CreateEncryptionKeyProvider();

}   // namespace NCloud::NBlockStore
