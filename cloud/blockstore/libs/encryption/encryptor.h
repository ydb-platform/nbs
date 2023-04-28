#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/encryption.pb.h>

#include <cloud/blockstore/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IEncryptor
{
    virtual ~IEncryptor() = default;

    virtual bool Encrypt(
        const TBlockDataRef& src,
        const TBlockDataRef& dst,
        ui64 blockIndex) = 0;

    virtual bool Decrypt(
        const TBlockDataRef& src,
        const TBlockDataRef& dst,
        ui64 blockIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IEncryptorPtr> CreateAesXtsEncryptor(
    const NProto::TKeyPath& encryptionKeyPath,
    TString& keyHash);

IEncryptorPtr CreateTestCaesarEncryptor(size_t shift);

TResultOrError<TString> ComputeEncryptionKeyHash(
    const NProto::TEncryptionSpec& encryptionSpec);

}   // namespace NCloud::NBlockStore
