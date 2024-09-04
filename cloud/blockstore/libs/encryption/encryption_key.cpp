#include "encryption_key.h"

#include <cloud/storage/core/libs/endpoints/keyring/keyring.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/builder.h>
#include <util/system/file.h>

#include <openssl/sha.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ComputeSHA384Hash(const TString& encryptionKey)
{
    SHA512_CTX ctx;
    SHA384_Init(&ctx);
    const ui8 version = 1;
    SHA384_Update(&ctx, &version, sizeof(version));
    ui32 keySize = encryptionKey.size();
    SHA384_Update(&ctx, &keySize, sizeof(keySize));
    SHA384_Update(&ctx, encryptionKey.data(), encryptionKey.size());

    TString hash;
    hash.resize(SHA384_DIGEST_LENGTH);
    SHA384_Final(reinterpret_cast<unsigned char*>(hash.Detach()), &ctx);
    return hash;
}

ui32 GetExpectedKeyLength(NProto::EEncryptionMode mode)
{
    switch (mode) {
        case NProto::NO_ENCRYPTION:
            return 0;
        case NProto::ENCRYPTION_AES_XTS:
            return 32;
        default:
            ythrow TServiceError(E_ARGUMENT)
                << "Unknown encryption mode: "
                << static_cast<int>(mode);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TEncryptionKeyProvider
    : public IEncryptionKeyProvider
{
private:
    const IKmsKeyProviderPtr KmsKeyProvider;

public:
    explicit TEncryptionKeyProvider(IKmsKeyProviderPtr kmsKeyProvider)
        : KmsKeyProvider(std::move(kmsKeyProvider))
    {}

    TFuture<TResponse> GetKey(
        const NProto::TEncryptionSpec& spec,
        const TString& diskId) override
    {
        const auto len = GetExpectedKeyLength(spec.GetMode());
        const auto& keyPath = spec.GetKeyPath();

        if (keyPath.HasKeyringId()) {
            return MakeFuture(ReadKeyFromKeyring(keyPath.GetKeyringId(), len));
        }

        if (keyPath.HasFilePath()) {
            return MakeFuture(ReadKeyFromFile(keyPath.GetFilePath(), len));
        }

        if (keyPath.HasKmsKey()) {
            return ReadKeyFromKMS(keyPath.GetKmsKey(), diskId, len);
        }

        return MakeFuture<TResponse>(TErrorResponse(
            E_ARGUMENT,
            "KeyPath should contain path to encryption key"));
    }

private:
    TResponse ReadKeyFromKeyring(ui32 keyringId, ui32 expectedLen)
    {
        return SafeExecute<TResponse>([&] () -> TResponse {
            auto keyring = TKeyring::Create(keyringId);

            if (keyring.GetValueSize() != expectedLen) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "Key from keyring " << keyringId
                    << " should has size " << expectedLen);
            }

            return TEncryptionKey(keyring.GetValue());
        });
    }

    TResponse ReadKeyFromFile(TString filePath, ui32 expectedLen)
    {
        return SafeExecute<TResponse>([&] () -> TResponse {
            TFile file(
                filePath,
                EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);

            if (file.GetLength() != expectedLen) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "Key file " << filePath.Quote()
                    << " size " << file.GetLength() << " != " << expectedLen);
            }

            TString key = TString::TUninitialized(expectedLen);
            auto size = file.Read(key.begin(), expectedLen);
            if (size != expectedLen) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "Read " << size << " bytes from key file "
                    << filePath.Quote() << ", expected " << expectedLen);
            }

            return TEncryptionKey(std::move(key));
        });
    }

    TFuture<TResponse> ReadKeyFromKMS(
        const NProto::TKmsKey& kmsKey,
        const TString& diskId,
        ui32 expectedLen)
    {
        auto future = KmsKeyProvider->GetKey(kmsKey, diskId);
        return future.Apply([diskId, expectedLen] (auto f) -> TResponse {
            auto response = f.ExtractValue();
            if (HasError(response)) {
                return response.GetError();
            }

            auto key = response.ExtractResult();
            if (key.GetKey().size() != expectedLen) {
                return MakeError(E_INVALID_STATE, TStringBuilder()
                    << "Key from KMS for disk " << diskId
                    << " should has size " << expectedLen);
            }

            return std::move(key);
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TKmsKeyProviderStub
    : public IKmsKeyProvider
{
public:
    TKmsKeyProviderStub() = default;

    TFuture<TResponse> GetKey(
        const NProto::TKmsKey& kmsKey,
        const TString& diskId) override
    {
        Y_UNUSED(kmsKey);
        Y_UNUSED(diskId);
        return MakeFuture<TResponse>(
            MakeError(E_ARGUMENT, "KmsKeyProviderStub can't get key"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEncryptionKey::TEncryptionKey(TString key)
    : Key(std::move(key))
{}

TEncryptionKey::~TEncryptionKey()
{
    SecureZero(Key.begin(), Key.Size());
}

const TString& TEncryptionKey::GetKey() const
{
    return Key;
}

TString TEncryptionKey::GetHash() const
{
    return Base64Encode(ComputeSHA384Hash(Key));
}

////////////////////////////////////////////////////////////////////////////////

IKmsKeyProviderPtr CreateKmsKeyProviderStub()
{
    return std::make_shared<TKmsKeyProviderStub>();
}

IEncryptionKeyProviderPtr CreateEncryptionKeyProvider(
    IKmsKeyProviderPtr kmsKeyProvider)
{
    return std::make_shared<TEncryptionKeyProvider>(
        std::move(kmsKeyProvider));
}

IEncryptionKeyProviderPtr CreateDefaultEncryptionKeyProvider()
{
    return CreateEncryptionKeyProvider(CreateKmsKeyProviderStub());
}

}   // namespace NCloud::NBlockStore
