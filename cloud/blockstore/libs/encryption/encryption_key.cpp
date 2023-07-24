#include "encryption_key.h"

#include <cloud/storage/core/libs/keyring/keyring.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/system/file.h>

#include <openssl/sha.h>

namespace NCloud::NBlockStore {

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
public:
    TEncryptionKeyProvider()
    {}

    TResultOrError<TEncryptionKey> GetKey(const NProto::TEncryptionSpec& spec)
    {
        return SafeExecute<TResultOrError<TEncryptionKey>>([&] {
            return GetEncryptionKey(
                spec.GetKeyPath(),
                GetExpectedKeyLength(spec.GetMode()));
        });
    }

private:
    TEncryptionKey GetEncryptionKey(NProto::TKeyPath keyPath, ui32 expectedLen)
    {
        if (keyPath.HasKeyringId()) {
            return ReadKeyFromKeyring(keyPath.GetKeyringId(), expectedLen);
        } else if (keyPath.HasFilePath()) {
            return ReadKeyFromFile(keyPath.GetFilePath(), expectedLen);
        } else if (keyPath.HasKmsKey()) {
            return ReadKeyFromKMS(keyPath.GetKmsKey(), expectedLen);
        } else {
            ythrow TServiceError(E_ARGUMENT)
                << "KeyPath should contain path to encryption key";
        }
    }

    TEncryptionKey ReadKeyFromKeyring(ui32 keyringId, ui32 expectedLen)
    {
        auto keyring = TKeyring::Create(keyringId);

        if (keyring.GetValueSize() != expectedLen) {
            ythrow TServiceError(E_ARGUMENT)
                << "Key from keyring " << keyringId
                << " should has size " << expectedLen;
        }

        return TEncryptionKey(keyring.GetValue());
    }

    TEncryptionKey ReadKeyFromFile(TString filePath, ui32 expectedLen)
    {
        TFile file(filePath, EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);

        if (file.GetLength() != expectedLen) {
            ythrow TServiceError(E_ARGUMENT)
                << "Key file " << filePath.Quote()
                << " size " << file.GetLength() << " != " << expectedLen;
        }

        TString key = TString::TUninitialized(expectedLen);
        auto size = file.Read(key.begin(), expectedLen);
        if (size != expectedLen) {
            ythrow TServiceError(E_ARGUMENT)
                << "Read " << size << " bytes from key file "
                << filePath.Quote() << ", expected " << expectedLen;
        }

        return TEncryptionKey(std::move(key));
    }

    TEncryptionKey ReadKeyFromKMS(const NProto::TKmsKey& kmsKey, ui32 expectedLen)
    {
        Y_UNUSED(kmsKey);
        Y_UNUSED(expectedLen);

        ythrow TServiceError(E_NOT_IMPLEMENTED)
            << "Reading key from KMS is not implemented yet";
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

IEncryptionKeyProviderPtr CreateEncryptionKeyProvider()
{
    return std::make_shared<TEncryptionKeyProvider>();
}

}   // namespace NCloud::NBlockStore
