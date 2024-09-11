#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IEncryptor;
using IEncryptorPtr = std::shared_ptr<IEncryptor>;

struct IEncryptionKeyProvider;
using IEncryptionKeyProviderPtr = std::shared_ptr<IEncryptionKeyProvider>;

struct IDefaultEncryptionKeyProvider;
using IDefaultEncryptionKeyProviderPtr =
    std::shared_ptr<IDefaultEncryptionKeyProvider>;

struct IKmsKeyProvider;
using IKmsKeyProviderPtr = std::shared_ptr<IKmsKeyProvider>;

struct IRootKmsKeyProvider;
using IRootKmsKeyProviderPtr = std::shared_ptr<IRootKmsKeyProvider>;

struct IEncryptionClientFactory;
using IEncryptionClientFactoryPtr = std::shared_ptr<IEncryptionClientFactory>;

}   // namespace NCloud::NBlockStore
