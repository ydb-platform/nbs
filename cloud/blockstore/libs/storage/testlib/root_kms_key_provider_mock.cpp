#include "root_kms_key_provider_mock.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRootKmsKeyProviderMock
    : IRootKmsKeyProvider
{
    auto GetKey(const NProto::TKmsKey& kmsKey, const TString& diskId)
        -> TFuture<TResultOrError<TEncryptionKey>> override
    {
        Y_UNUSED(kmsKey);
        Y_UNUSED(diskId);

        return MakeFuture<TResultOrError<TEncryptionKey>>(
            TEncryptionKey{"12345678901234567890123456789012"});
    }

    auto GenerateDataEncryptionKey(const TString& diskId)
        -> TFuture<TResultOrError<NProto::TKmsKey>> override
    {
        Y_UNUSED(diskId);

        NProto::TKmsKey key;
        key.SetKekId("nbs");
        key.SetEncryptedDEK("encrypted-DEK");
        return MakeFuture<TResultOrError<NProto::TKmsKey>>(key);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRootKmsKeyProviderPtr CreateRootKmsKeyProviderMock()
{
    return std::make_shared<TRootKmsKeyProviderMock>();
}

}   // namespace NCloud::NBlockStore::NStorage
