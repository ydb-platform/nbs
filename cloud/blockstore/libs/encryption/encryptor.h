#pragma once

#include "public.h"

#include "encryption_key.h"

#include <cloud/blockstore/libs/common/block_data_ref.h>

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

IEncryptorPtr CreateAesXtsEncryptor(TEncryptionKey key);

IEncryptorPtr CreateTestCaesarEncryptor(size_t shift);

}   // namespace NCloud::NBlockStore
