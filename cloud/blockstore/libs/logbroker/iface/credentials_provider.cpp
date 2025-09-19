#include "credentials_provider.h"

#include <cloud/blockstore/libs/logbroker/iface/config.h>

#include <contrib/ydb/public/sdk/cpp/client/iam/common/iam.h>

#include <util/generic/overloaded.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NYdbICredentialsProviderFactory>
CreateCredentialsProviderFactory(const TLogbrokerConfig& config)
{
    return std::visit(TOverloaded(
        [] (const TIamJwtFile& jwt) {
            NYdb::TIamJwtFilename iamJwtFilename;
            iamJwtFilename.Endpoint = jwt.GetIamEndpoint();
            iamJwtFilename.JwtFilename = jwt.GetJwtFilename();

            return NYdb::CreateIamJwtFileCredentialsProviderFactory(
                iamJwtFilename);

        }, [] (const TIamMetadataServer& server) {
            TStringBuf hostRef;
            TStringBuf portRef;
            TStringBuf{server.GetEndpoint()}.Split(':', hostRef, portRef);

            return NYdb::CreateIamCredentialsProviderFactory({
                .Host = TString(hostRef),
                .Port = FromString<ui32>(portRef),
            });

        }, [] (std::monostate) {
            return std::shared_ptr<NYdbICredentialsProviderFactory>();
        }), config.GetAuthConfig());
}

}   // namespace NCloud::NBlockStore::NLogbroker
