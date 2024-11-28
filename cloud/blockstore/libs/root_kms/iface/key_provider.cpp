#include "key_provider.h"

#include "client.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/storage/core/libs/coroutine/executor.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRootKmsKeyProvider
    : public IRootKmsKeyProvider
{
private:
    const IRootKmsClientPtr Client;
    const TString KeyId;

public:
    TRootKmsKeyProvider(
            IRootKmsClientPtr client,
            TString keyId)
        : Client(std::move(client))
        , KeyId(std::move(keyId))
    {}

    auto GetKey(const NProto::TKmsKey& kmsKey, const TString& diskId)
        -> TFuture<TResultOrError<TEncryptionKey>> override
    {
        Y_UNUSED(diskId);

        return Client->Decrypt(kmsKey.GetKekId(), kmsKey.GetEncryptedDEK());
    }

    auto GenerateDataEncryptionKey(const TString& diskId)
        -> TFuture<TResultOrError<NProto::TKmsKey>> override
    {
        Y_UNUSED(diskId);

        return Client->GenerateDataEncryptionKey(KeyId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRootKmsKeyProviderPtr CreateRootKmsKeyProvider(
    IRootKmsClientPtr client,
    TString keyId)
{
    return std::make_shared<TRootKmsKeyProvider>(
        std::move(client),
        std::move(keyId));
}

}   // namespace NCloud::NBlockStore
