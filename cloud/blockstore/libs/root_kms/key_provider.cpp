#include "key_provider.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/random/entropy.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> SafeBase64Decode(TStringBuf encoded)
{
    try {
        return Base64Decode(encoded);
    } catch (...) {
        return MakeError(E_ARGUMENT, CurrentExceptionMessage());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TRootKmsKeyProvider final
    : public IRootKmsKeyProvider
{
public:
    auto GetKey(const NProto::TKmsKey& kmsKey, const TString& diskId)
        -> TFuture<TResultOrError<TEncryptionKey>> override
    {
        if (kmsKey.GetKekId()) {
            return MakeFuture<TResultOrError<TEncryptionKey>>(
                MakeError(E_NOT_IMPLEMENTED, "TODO: get DEK from Root KMS"));
        }

        auto [key, error] = SafeBase64Decode(kmsKey.GetEncryptedDEK());
        if (HasError(error)) {
            return MakeFuture<TResultOrError<TEncryptionKey>>(MakeError(
                error.GetCode(),
                TStringBuilder() << "failed to decode DEK for disk " << diskId
                                 << ", error: " << error));
        }

        return MakeFuture<TResultOrError<TEncryptionKey>>(TEncryptionKey(key));
    }

    auto GenerateDataEncryptionKey(const TString& diskId)
        -> TFuture<TResultOrError<NProto::TKmsKey>> override
    {
        Y_UNUSED(diskId);

        // TODO: use Root KMS client

        try {
            TString key;
            key.resize(32);
            EntropyPool().Read(key.Detach(), key.size());

            NProto::TKmsKey kmsKey;
            kmsKey.SetEncryptedDEK(Base64Encode(key));

            return MakeFuture<TResultOrError<NProto::TKmsKey>>(
                std::move(kmsKey));

        } catch (...) {
            return MakeFuture<TResultOrError<NProto::TKmsKey>>(MakeError(
                E_FAIL,
                TStringBuilder() << "failed to generate DEK for disk " << diskId
                                 << ", error: " << CurrentExceptionMessage()));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRootKmsKeyProviderPtr CreateRootKmsKeyProvider()
{
    return std::make_shared<TRootKmsKeyProvider>();
}

}   // namespace NCloud::NBlockStore
