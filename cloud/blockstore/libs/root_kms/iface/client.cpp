#include "client.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/public/api/protos/encryption.pb.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRootKmsClientStub: public IRootKmsClient
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    auto Decrypt(const TString& keyId, const TString& ciphertext)
        -> TFuture<TResultOrError<TEncryptionKey>> override
    {
        Y_UNUSED(keyId);
        Y_UNUSED(ciphertext);
        return MakeFuture<TResultOrError<TEncryptionKey>>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            "RootKmsClientStub can't decrypt cyphertext"));
    }

    auto GenerateDataEncryptionKey(const TString& keyId)
        -> TFuture<TResultOrError<NProto::TKmsKey>> override
    {
        Y_UNUSED(keyId);
        return MakeFuture<TResultOrError<NProto::TKmsKey>>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            "RootKmsClientStub can't generate DEK"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRootKmsClientPtr CreateRootKmsClientStub()
{
    return std::make_shared<TRootKmsClientStub>();
}

}   // namespace NCloud::NBlockStore
