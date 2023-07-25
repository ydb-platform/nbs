#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IEncryptor;
using IEncryptorPtr = std::shared_ptr<IEncryptor>;

struct IEncryptionKeyProvider;
using IEncryptionKeyProviderPtr = std::shared_ptr<IEncryptionKeyProvider>;

struct IKmsKeyProvider;
using IKmsKeyProviderPtr = std::shared_ptr<IKmsKeyProvider>;

}   // namespace NCloud::NBlockStore
