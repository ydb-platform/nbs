#include "keyring_endpoints.h"

#include "keyring.h"

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/tempfile.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TKeyringStorage final
    : public IEndpointStorage
{
private:
    const TString RootKeyringDesc;
    const TString EndpointsKeyringDesc;
    const bool NotImplementedErrorIsFatal;

public:
    TKeyringStorage(
            TString rootKeyringDesc,
            TString endpointsKeyringDesc,
            bool notImplementedErrorIsFatal)
        : RootKeyringDesc(std::move(rootKeyringDesc))
        , EndpointsKeyringDesc(std::move(endpointsKeyringDesc))
        , NotImplementedErrorIsFatal(notImplementedErrorIsFatal)
    {}

    TResultOrError<TVector<TString>> GetEndpointIds() override
    {
        auto keyringsOrError = GetEndpointKeyrings();
        if (HasError(keyringsOrError)) {
            return keyringsOrError.GetError();
        }

        auto keyrings = keyringsOrError.ExtractResult();

        TVector<TString> endpointIds;
        for (auto keyring: keyrings) {
            endpointIds.push_back(ToString(keyring.GetId()));
        }
        return endpointIds;
    }

    TResultOrError<TString> GetEndpoint(const TString& endpointId) override
    {
        auto keyringsOrError = GetEndpointKeyrings();
        if (HasError(keyringsOrError)) {
            return keyringsOrError.GetError();
        }

        auto keyrings = keyringsOrError.ExtractResult();
        for (auto keyring: keyrings) {
            if (ToString(keyring.GetId()) == endpointId) {
                return GetKeyringValue(keyring);
            }
        }

        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Failed to find endpoint with id " << endpointId);
    }

    NProto::TError AddEndpoint(
        const TString& endpointId,
        const TString& endpointSpec) override
    {
        Y_UNUSED(endpointId);
        Y_UNUSED(endpointSpec);
        // TODO:
        return NotImplemented("Failed to add endpoint to storage");
    }

    NProto::TError RemoveEndpoint(const TString& endpointId) override
    {
        Y_UNUSED(endpointId);
        // TODO:
        return NotImplemented("Failed to remove endpoint from storage");
    }

private:
    NProto::TError NotImplemented(TString message) const
    {
        ui32 flags = 0;
        if (!NotImplementedErrorIsFatal) {
            SetProtoFlag(flags, NProto::EF_SILENT);
        }
        return MakeError(E_NOT_IMPLEMENTED, std::move(message), flags);
    }

    TResultOrError<TVector<TKeyring>> GetEndpointKeyrings()
    {
        if (RootKeyringDesc.empty() || EndpointsKeyringDesc.empty()) {
            return TVector<TKeyring>();
        }

        return SafeExecute<TResultOrError<TVector<TKeyring>>>([&] {
            auto rootKeyring = TKeyring::GetProcKey(RootKeyringDesc);
            if (!rootKeyring) {
                STORAGE_THROW_SERVICE_ERROR(E_INVALID_STATE)
                    << "Failed to find root keyring "
                    << RootKeyringDesc.Quote();
            }

            auto endpointsKeyring = rootKeyring.SearchKeyring(
                EndpointsKeyringDesc);

            if (!endpointsKeyring) {
                STORAGE_THROW_SERVICE_ERROR(E_INVALID_STATE)
                    << "Failed to find endpoints keyring "
                    << EndpointsKeyringDesc.Quote();
            }

            return endpointsKeyring.GetUserKeys();
        });
    }

    TResultOrError<TString> GetKeyringValue(TKeyring keyring)
    {
        return SafeExecute<TResultOrError<TString>>([&] {
            return keyring.GetValue();
        });
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointStoragePtr CreateKeyringEndpointStorage(
    TString rootKeyringDesc,
    TString endpointsKeyringDesc,
    bool notImplementedErrorIsFatal)
{
    return std::make_shared<TKeyringStorage>(
        std::move(rootKeyringDesc),
        std::move(endpointsKeyringDesc),
        notImplementedErrorIsFatal);
}

}   // namespace NCloud
