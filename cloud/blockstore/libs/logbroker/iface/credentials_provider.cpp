#include "credentials_provider.h"

#include <cloud/blockstore/libs/logbroker/iface/config.h>

#include <contrib/ydb/public/sdk/cpp/client/iam/common/iam.h>

#include <util/string/cast.h>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NYdbICredentialsProviderFactory>
CreateCredentialsProviderFactory(const TLogbrokerConfig& config)
{
    auto authConfig = config.GetAuthConfig();
    static_assert(std::variant_size_v<decltype(authConfig)> == 3);

    if (const auto* jwt = std::get_if<TIamJwtFile>(&authConfig)) {
        NYdb::TIamJwtFilename iamJwtFilename;
        iamJwtFilename.Endpoint = jwt->GetIamEndpoint();
        iamJwtFilename.JwtFilename = jwt->GetJwtFilename();

        return NYdb::CreateIamJwtFileCredentialsProviderFactory(iamJwtFilename);
    }

    if (const auto* server = std::get_if<TIamMetadataServer>(&authConfig)) {
        TStringBuf hostRef;
        TStringBuf portRef;
        TStringBuf{server->GetEndpoint()}.Split(':', hostRef, portRef);

        return NYdb::CreateIamCredentialsProviderFactory({
            .Host = TString(hostRef),
            .Port = FromString<ui32>(portRef),
        });
    }

    Y_ABORT_UNLESS(std::holds_alternative<std::monostate>(authConfig));

    return std::shared_ptr<NYdbICredentialsProviderFactory>();
}

}   // namespace NCloud::NBlockStore::NLogbroker
