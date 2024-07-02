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

struct IEncryptionClientFactory;
using IEncryptionClientFactoryPtr = std::shared_ptr<IEncryptionClientFactory>;

struct IVolumeEncryptionClientFactory;
using IVolumeEncryptionClientFactoryPtr = std::shared_ptr<IVolumeEncryptionClientFactory>;

}   // namespace NCloud::NBlockStore
