#include "encryptor.h"

#include <util/generic/utility.h>
#include <util/system/sanitizers.h>

#include <openssl/err.h>
#include <openssl/evp.h>

namespace NCloud::NBlockStore {

namespace {

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEncryptorPtr CreateAesXtsEncryptor(TEncryptionKey key)
{
    return std::make_shared<TAesXtsEncryptor>(key.GetKey());
}

IEncryptorPtr CreateTestCaesarEncryptor(size_t shift)
{
    return std::make_shared<TCaesarEncryptor>(shift);
}

}   // namespace NCloud::NBlockStore
