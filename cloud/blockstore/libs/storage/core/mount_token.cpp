#include "mount_token.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/random/entropy.h>
#include <util/stream/input.h>
#include <util/stream/str.h>

#include <openssl/crypto.h>
#include <openssl/sha.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void ComputeSHA384Hash(
    const TString& salt,
    const TString& secret,
    TString& hash)
{
    SHA512_CTX ctx;
    SHA384_Init(&ctx);
    ui64 saltSize = salt.size();
    SHA384_Update(&ctx, &saltSize, sizeof(saltSize));
    SHA384_Update(&ctx, salt.data(), salt.size());
    ui64 secretSize = secret.size();
    SHA384_Update(&ctx, &secretSize, sizeof(secretSize));
    SHA384_Update(&ctx, secret.data(), secret.size());
    hash.resize(SHA384_DIGEST_LENGTH);
    SHA384_Final(reinterpret_cast<unsigned char*>(hash.Detach()), &ctx);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TMountToken::SetSecret(EFormat format, const TString& secret)
{
    TString salt;
    switch (format) {
        case EFormat::EMPTY:
            Y_ABORT("Cannot set secret when format is empty");
        case EFormat::SHA384_V1:
            salt.resize(16);
            EntropyPool().Read(salt.Detach(), salt.size());
            break;
    }
    SetSecret(format, secret, salt);
}

void TMountToken::SetSecret(
    EFormat format,
    const TString& secret,
    const TString& salt)
{
    switch (format) {
        case EFormat::EMPTY:
            Y_ABORT("Cannot set secret when format is empty");
        case EFormat::SHA384_V1:
            Format = format;
            Salt = salt;
            ComputeSHA384Hash(salt, secret, Hash);
            return;
    }
    Y_UNREACHABLE();
}

bool TMountToken::VerifySecret(const TString& secret) const
{
    switch (Format) {
        case EFormat::EMPTY:
            return secret.empty();
        case EFormat::SHA384_V1: {
            TString hash;
            ComputeSHA384Hash(Salt, secret, hash);
            Y_ABORT_UNLESS(Hash.size() == SHA384_DIGEST_LENGTH);
            Y_ABORT_UNLESS(hash.size() == SHA384_DIGEST_LENGTH);
            return CRYPTO_memcmp(Hash.data(), hash.data(), Hash.size()) == 0;
        }
    }
    Y_UNREACHABLE();
}

TString TMountToken::ToString() const
{
    TString result;
    TStringOutput out(result);
    switch (Format) {
        case EFormat::EMPTY:
            break;
        case EFormat::SHA384_V1:
            out << "SHA384_V1" << ':' << Base64Encode(Salt) << ':'
                << Base64Encode(Hash);
            break;
    }
    return result;
}

TMountToken::EStatus TMountToken::ParseString(const TString& token)
{
    if (token.empty()) {
        Format = EFormat::EMPTY;
        Salt.clear();
        Hash.clear();
        return EStatus::OK;
    }
    size_t pos = token.find(':');
    if (pos == TString::npos) {
        return EStatus::FORMAT_UNKNOWN;
    }
    auto format = token.substr(0, pos);
    if (format == "SHA384_V1") {
        Format = EFormat::SHA384_V1;
    } else {
        return EStatus::FORMAT_UNKNOWN;
    }
    size_t pos2 = token.find(':', pos + 1);
    if (pos2 == TString::npos) {
        return EStatus::TOKEN_CORRUPTED;
    }
    try {
        Salt = Base64StrictDecode(token.substr(pos + 1, pos2 - pos - 1));
        Hash = Base64StrictDecode(token.substr(pos2 + 1));
    } catch (const yexception&) {
        return EStatus::TOKEN_CORRUPTED;
    }
    if (Hash.size() != SHA384_DIGEST_LENGTH) {
        return EStatus::TOKEN_CORRUPTED;
    }
    return EStatus::OK;
}

}   // namespace NCloud::NBlockStore::NStorage
