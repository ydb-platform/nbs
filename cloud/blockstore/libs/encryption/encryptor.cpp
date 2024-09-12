#include "encryptor.h"

#include <util/generic/utility.h>
#include <util/string/builder.h>
#include <util/system/sanitizers.h>

#include <openssl/err.h>
#include <openssl/evp.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeOpenSSLError(TStringBuf func)
{
    const ui32 err = ERR_get_error();
    char message[256] {};
    ERR_error_string_n(err, message, sizeof(message));

    return MakeError(
        MAKE_SYSTEM_ERROR(err),
        TStringBuilder() << func << " failed: " << message);
}

NProto::TError MakeSourceBufferTooLargeError(TBlockDataRef buf)
{
    return MakeError(
        E_ARGUMENT,
        TStringBuilder() << "the source buffer is too large: " << buf.Size());
}

NProto::TError MakeDifferentSizesError(TBlockDataRef src, TBlockDataRef dst)
{
    return MakeError(
        E_ARGUMENT,
        TStringBuilder()
            << "the source and target buffers have different sizes: "
            << src.Size() << " != " << dst.Size());
}

NProto::TError MakeWrongTotalLengthError(int totalLen, int srcLen)
{
    return MakeError(
        E_INVALID_STATE,
        TStringBuilder()
            << "the total length is not equal to the size of the source "
               "buffer: "
            << totalLen << " != " << srcLen);
}

////////////////////////////////////////////////////////////////////////////////

class TAesXtsEncryptor final
    : public IEncryptor
{
    using TEvpCipherCtxPtr =
        std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;

private:
    unsigned char Key[EVP_MAX_KEY_LENGTH] = {};

public:
    explicit TAesXtsEncryptor(TStringBuf key)
    {
        memcpy(Key, key.Data(), Min<ui32>(key.size(), EVP_MAX_KEY_LENGTH));
    }

    ~TAesXtsEncryptor() override
    {
        SecureZero(Key, EVP_MAX_KEY_LENGTH);
    }

    NProto::TError Encrypt(
        TBlockDataRef srcRef,
        TBlockDataRef dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return MakeDifferentSizesError(srcRef, dstRef);
        }

        if (srcRef.Size() > INT_MAX) {
            return MakeSourceBufferTooLargeError(srcRef);
        }

        if (srcRef.Data() == nullptr) {
            return MakeError(E_ARGUMENT, "the source buffer is null");
        }

        const int srcLen = static_cast<int>(srcRef.Size());
        const auto* src = reinterpret_cast<const unsigned char*>(srcRef.Data());
        auto* dst =
            reinterpret_cast<unsigned char*>(const_cast<char*>(dstRef.Data()));
        unsigned char iv[EVP_MAX_IV_LENGTH] {};

        memcpy(iv, &blockIndex, sizeof(blockIndex));

        TEvpCipherCtxPtr ctx(EVP_CIPHER_CTX_new(), ::EVP_CIPHER_CTX_free);

        if (!EVP_EncryptInit_ex(ctx.get(), EVP_aes_128_xts(), nullptr, Key, iv)) {
            return MakeOpenSSLError("EVP_EncryptInit_ex");
        }

        int len = 0;
        if (!EVP_EncryptUpdate(ctx.get(), dst, &len, src, srcLen)) {
            return MakeOpenSSLError("EVP_EncryptUpdate");
        }

        int totalLen = len;
        if (!EVP_EncryptFinal_ex(ctx.get(), dst + len, &len)) {
            return MakeOpenSSLError("EVP_EncryptFinal_ex");
        }

        totalLen += len;
        if (totalLen != srcLen) {
            return MakeWrongTotalLengthError(totalLen, srcLen);
        }

        NSan::Unpoison(dstRef.Data(), dstRef.Size());

        return {};
    }

    NProto::TError Decrypt(
        TBlockDataRef srcRef,
        TBlockDataRef dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return MakeDifferentSizesError(srcRef, dstRef);
        }

        if (srcRef.Size() > INT_MAX) {
            return MakeSourceBufferTooLargeError(srcRef);
        }

        if (srcRef.Data() == nullptr) {
            memset(const_cast<char*>(dstRef.Data()), 0, srcRef.Size());
            return {};
        }

        const int srcLen = static_cast<int>(srcRef.Size());
        auto src = reinterpret_cast<const unsigned char*>(srcRef.Data());
        auto dst =
            reinterpret_cast<unsigned char*>(const_cast<char*>(dstRef.Data()));
        unsigned char iv[EVP_MAX_IV_LENGTH] {};

        memcpy(iv, &blockIndex, sizeof(blockIndex));

        TEvpCipherCtxPtr ctx(EVP_CIPHER_CTX_new(), ::EVP_CIPHER_CTX_free);

        if (!EVP_DecryptInit_ex(ctx.get(), EVP_aes_128_xts(), nullptr, Key, iv)) {
            return MakeOpenSSLError("EVP_DecryptInit_ex");
        }

        int len = 0;
        if (!EVP_DecryptUpdate(ctx.get(), dst, &len, src, srcLen)) {
            return MakeOpenSSLError("EVP_DecryptUpdate");
        }

        int totalLen = len;
        if (!EVP_DecryptFinal_ex(ctx.get(), dst + len, &len)) {
            return MakeOpenSSLError("EVP_DecryptFinal_ex");
        }

        totalLen += len;
        if (totalLen != srcLen) {
            return MakeWrongTotalLengthError(totalLen, srcLen);
        }

        NSan::Unpoison(dstRef.Data(), dstRef.Size());

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCaesarEncryptor final
    : public IEncryptor
{
private:
    const size_t Shift;

public:
    explicit TCaesarEncryptor(size_t shift)
        : Shift(shift)
    {}

    NProto::TError Encrypt(
        TBlockDataRef srcRef,
        TBlockDataRef dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return MakeDifferentSizesError(srcRef, dstRef);
        }

        if (srcRef.Data() == nullptr) {
            return MakeError(E_ARGUMENT, "the source buffer is null");
        }

        const char* src = srcRef.Data();
        char* dst = const_cast<char*>(dstRef.Data());
        for (size_t i = 0; i < dstRef.Size(); ++i) {
            dst[i] = src[i] + Shift + blockIndex;
        }

        return {};
    }

    NProto::TError Decrypt(
        TBlockDataRef srcRef,
        TBlockDataRef dstRef,
        ui64 blockIndex) override
    {
        if (srcRef.Size() != dstRef.Size()) {
            return MakeDifferentSizesError(srcRef, dstRef);
        }

        if (srcRef.Size() > INT_MAX) {
            return MakeSourceBufferTooLargeError(srcRef);
        }

        if (srcRef.Data() == nullptr) {
            memset(const_cast<char*>(dstRef.Data()), 0, srcRef.Size());
            return {};
        }

        const char* src = srcRef.Data();
        char* dst = const_cast<char*>(dstRef.Data());
        for (size_t i = 0; i < dstRef.Size(); ++i) {
            dst[i] = src[i] - Shift - blockIndex;
        }

        return {};
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
