#include "kms_client.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TKmsClientStub: public IKmsClient
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TFuture<TResponse> Decrypt(
        const TString& keyId,
        const TString& ciphertext,
        const TString& authToken) override
    {
        Y_UNUSED(keyId);
        Y_UNUSED(ciphertext);
        Y_UNUSED(authToken);
        return MakeFuture<TResponse>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            "KmsClientStub can't decrypt cyphertext"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IKmsClientPtr CreateKmsClientStub()
{
    return std::make_shared<TKmsClientStub>();
}

}   // namespace NCloud::NBlockStore
