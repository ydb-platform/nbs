#include "utils.h"

#include <util/generic/map.h>
#include <util/generic/yexception.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TMap<TString, NProto::EEncryptionMode> EncryptionModes = {
    {"no", NProto::NO_ENCRYPTION},
    {"aes-xts", NProto::ENCRYPTION_AES_XTS},
    {"default", NProto::ENCRYPTION_AT_REST},
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::EEncryptionMode EncryptionModeFromString(const TString& str)
{
    auto it = EncryptionModes.find(str);
    if (it != EncryptionModes.end()) {
        return it->second;
    }

    ythrow yexception() << "invalid encryption mode: " << str;
}

TString EncryptionModeToString(NProto::EEncryptionMode encryptionMode)
{
    for (const auto& [key, value]: EncryptionModes) {
        if (value == encryptionMode) {
            return key;
        }
    }
    ythrow yexception() << "invalid encryption mode: "
                        << static_cast<int>(encryptionMode);
}

NProto::TEncryptionSpec CreateEncryptionSpec(
    NProto::EEncryptionMode mode,
    const TString& keyPath,
    const TString& keyHash)
{
    if (mode == NProto::NO_ENCRYPTION || mode == NProto::ENCRYPTION_AT_REST) {
        Y_ENSURE(
            keyHash.empty() && keyPath.empty(),
            "invalid encryption options: set aes-xts encryption mode or remove "
            "key hash and key path");

        NProto::TEncryptionSpec encryptionSpec;
        encryptionSpec.SetMode(mode);
        return encryptionSpec;
    }

    Y_ENSURE(
        keyHash.empty() || keyPath.empty(),
        "invalid encryption options: set key path or key hash, not both");

    Y_ENSURE(
        keyHash || keyPath,
        "invalid encryption options: set key hash or key path or remove "
        "encryption mode");

    NProto::TEncryptionSpec encryptionSpec;
    encryptionSpec.SetMode(mode);
    encryptionSpec.SetKeyHash(keyHash);
    encryptionSpec.MutableKeyPath()->SetFilePath(keyPath);
    return encryptionSpec;
}

}   // namespace NCloud::NBlockStore
