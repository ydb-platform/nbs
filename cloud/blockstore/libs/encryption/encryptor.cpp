#include "encryptor.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/keyring/keyring.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/utility.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/sanitizers.h>

#include <openssl/err.h>
#include <openssl/evp.h>
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

////////////////////////////////////////////////////////////////////////////////

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

class TAesXtsEncryptor
    : public IEncryptor
{
    using TEvpCipherCtxPtr =
        std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;

private:
    unsigned char Key[EVP_MAX_KEY_LENGTH] = {0};

public:
    TAesXtsEncryptor(const TString& key)
    {
        memcpy(Key, key.Data(), Min<ui32>(key.size(), EVP_MAX_KEY_LENGTH));
    }

    ~TAesXtsEncryptor()
    {
        SecureZero(Key, EVP_MAX_KEY_LENGTH);
    }

    bool Encrypt(
        const TBlockDataRef& srcRef,
        const TBlockDataRef& dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return false;
        }

        if (srcRef.Data() == nullptr) {
            return false;
        }

        auto src = reinterpret_cast<const unsigned char*>(srcRef.Data());
        auto dst = reinterpret_cast<unsigned char*>(const_cast<char*>(dstRef.Data()));
        unsigned char iv[EVP_MAX_IV_LENGTH] = {0};
        memcpy(iv, &blockIndex, sizeof(blockIndex));

        auto ctx = TEvpCipherCtxPtr(EVP_CIPHER_CTX_new(), ::EVP_CIPHER_CTX_free);

        auto res = EVP_EncryptInit_ex(ctx.get(), EVP_aes_128_xts(), NULL, Key, iv);
        if (res != 1) {
            return false;
        }

        int len = 0;
        res = EVP_EncryptUpdate(ctx.get(), dst, &len, src, srcRef.Size());
        if (res != 1) {
            return false;
        }

        size_t totalLen = len;
        res = EVP_EncryptFinal_ex(ctx.get(), dst + len, &len);
        if (res != 1) {
            return false;
        }

        totalLen += len;
        if (totalLen != srcRef.Size()) {
            return false;
        }

        NSan::Unpoison(dstRef.Data(), dstRef.Size());
        return true;
    }

    bool Decrypt(
        const TBlockDataRef& srcRef,
        const TBlockDataRef& dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return false;
        }

        if (srcRef.Data() == nullptr) {
            memset(const_cast<char*>(dstRef.Data()), 0, srcRef.Size());
            return true;
        }

        auto src = reinterpret_cast<const unsigned char*>(srcRef.Data());
        auto dst = reinterpret_cast<unsigned char*>(const_cast<char*>(dstRef.Data()));
        unsigned char iv[EVP_MAX_IV_LENGTH] = {0};
        memcpy(iv, &blockIndex, sizeof(blockIndex));

        auto ctx = TEvpCipherCtxPtr(EVP_CIPHER_CTX_new(), ::EVP_CIPHER_CTX_free);

        auto res = EVP_DecryptInit_ex(ctx.get(), EVP_aes_128_xts(), NULL, Key, iv);
        if (res != 1) {
            return false;
        }

        int len = 0;
        res = EVP_DecryptUpdate(ctx.get(), dst, &len, src, srcRef.Size());
        if (res != 1) {
            return false;
        }

        size_t totalLen = len;
        res = EVP_DecryptFinal_ex(ctx.get(), dst + len, &len);
        if (res != 1) {
            return false;
        }

        totalLen += len;
        if (totalLen != srcRef.Size()) {
            return false;
        }

        NSan::Unpoison(dstRef.Data(), dstRef.Size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCaesarEncryptor
    : public IEncryptor
{
private:
    const size_t Shift;

public:
    TCaesarEncryptor(size_t shift)
        : Shift(shift)
    {}

    bool Encrypt(
        const TBlockDataRef& srcRef,
        const TBlockDataRef& dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return false;
        }

        if (srcRef.Data() == nullptr) {
            return false;
        }

        const char* src = srcRef.Data();
        char* dst = const_cast<char*>(dstRef.Data());
        for (size_t i = 0; i < dstRef.Size(); ++i) {
            dst[i] = src[i] + Shift + blockIndex;
        }

        return true;
    }

    bool Decrypt(
        const TBlockDataRef& srcRef,
        const TBlockDataRef& dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return false;
        }

        if (srcRef.Data() == nullptr) {
            memset(const_cast<char*>(dstRef.Data()), 0, srcRef.Size());
            return true;
        }

        const char* src = srcRef.Data();
        char* dst = const_cast<char*>(dstRef.Data());
        for (size_t i = 0; i < dstRef.Size(); ++i) {
            dst[i] = src[i] - Shift - blockIndex;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEncryptionKey
{
private:
    TString Key;

public:
    TEncryptionKey(TString key = "")
        : Key(std::move(key))
    {}

    ~TEncryptionKey()
    {
        SecureZero(Key.begin(), Key.Size());
    }

    const TString& Get() const
    {
        return Key;
    }

    TString GetHash() const
    {
        return Base64Encode(ComputeSHA384Hash(Key));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEncryptionKey ReadKeyFromKeyring(ui32 keyringId, ui32 expectedLength)
{
    auto keyring = TKeyring::Create(keyringId);

    if (keyring.GetValueSize() != expectedLength) {
        ythrow TServiceError(E_ARGUMENT)
            << "Key from keyring " << keyringId
            << " should has size " << expectedLength;
    }

    return TEncryptionKey(keyring.GetValue());
}

TEncryptionKey ReadKeyFromFile(TString filePath, ui32 expectedLength)
{
    TFile file(filePath, EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);

    if (file.GetLength() != expectedLength) {
        ythrow TServiceError(E_ARGUMENT)
            << "Key file " << filePath.Quote()
            << " size " << file.GetLength() << " != " << expectedLength;
    }

    TString key = TString::TUninitialized(expectedLength);
    auto size = file.Read(key.begin(), expectedLength);
    if (size != expectedLength) {
        ythrow TServiceError(E_ARGUMENT)
            << "Read " << size << " bytes from key file "
            << filePath.Quote() << ", expected " << expectedLength;
    }

    return TEncryptionKey(std::move(key));
}

TEncryptionKey GetEncryptionKey(NProto::TKeyPath keyPath, ui32 expectedLength)
{
    if (keyPath.HasKeyringId()) {
        return ReadKeyFromKeyring(keyPath.GetKeyringId(), expectedLength);
    } else if (keyPath.HasFilePath()) {
        return ReadKeyFromFile(keyPath.GetFilePath(), expectedLength);
    } else {
        ythrow TServiceError(E_ARGUMENT)
            << "KeyPath should contain path to encryption key";
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IEncryptorPtr> CreateAesXtsEncryptor(
    const NProto::TKeyPath& encryptionKeyPath,
    TString& keyHash)
{
    return SafeExecute<TResultOrError<IEncryptorPtr>>([&] {
        auto expectedLength = GetExpectedKeyLength(NProto::ENCRYPTION_AES_XTS);
        auto key = GetEncryptionKey(encryptionKeyPath, expectedLength);
        keyHash = key.GetHash();
        IEncryptorPtr encryptor = std::make_shared<TAesXtsEncryptor>(key.Get());
        return encryptor;
    });
}

IEncryptorPtr CreateTestCaesarEncryptor(size_t shift)
{
    return std::make_shared<TCaesarEncryptor>(shift);
}

TResultOrError<TString> ComputeEncryptionKeyHash(
    const NProto::TEncryptionSpec& spec)
{
    if (spec.GetMode() == NProto::NO_ENCRYPTION) {
        if (spec.HasKeyHash() || spec.HasKeyPath()) {
            return MakeError(
                E_ARGUMENT,
                "For not encrypted mode KeyHash and KeyPath should be empty");
        }
        return TString();
    }

    if (spec.HasKeyHash()) {
        return spec.GetKeyHash();
    }

    if (spec.HasKeyPath()) {
        return SafeExecute<TResultOrError<TString>>([&] {
            auto expectedKeyLength = GetExpectedKeyLength(spec.GetMode());
            auto key = GetEncryptionKey(spec.GetKeyPath(), expectedKeyLength);
            return key.GetHash();
        });
    }

    return TString();
}

}   // namespace NCloud::NBlockStore
