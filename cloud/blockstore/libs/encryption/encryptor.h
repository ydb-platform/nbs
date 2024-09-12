#pragma once

#include "public.h"

#include "encryption_key.h"

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IEncryptor
{
    virtual ~IEncryptor() = default;

    virtual NProto::TError Encrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) = 0;

    virtual NProto::TError Decrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IEncryptorPtr CreateAesXtsEncryptor(TEncryptionKey key);

IEncryptorPtr CreateTestCaesarEncryptor(size_t shift);

}   // namespace NCloud::NBlockStore
