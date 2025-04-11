#pragma once

#include <cloud/blockstore/public/api/protos/encryption.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NProto::EEncryptionMode EncryptionModeFromString(const TString& str);

TString EncryptionModeToString(NProto::EEncryptionMode encryptionMode);

NProto::TEncryptionSpec CreateEncryptionSpec(
    NProto::EEncryptionMode mode,
    const TString& keyPath,
    const TString& keyHash);

}   // namespace NCloud::NBlockStore
