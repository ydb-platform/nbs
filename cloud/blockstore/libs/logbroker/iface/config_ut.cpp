#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogbrokerConfigTest)
{
    Y_UNIT_TEST(ShouldSetupProviderConfigFromMetadataServerAddress)
    {
        const TString metadataServerAddress = "localhost:123";

        NProto::TLogbrokerConfig proto;
        proto.SetMetadataServerAddress(metadataServerAddress);

        TLogbrokerConfig config{proto};
        UNIT_ASSERT_VALUES_EQUAL(
            metadataServerAddress,
            config.GetMetadataServerAddress());

        auto auth = config.GetAuthConfig();

        auto& metadataServerConfig = std::get<TIamMetadataServer>(auth);

        UNIT_ASSERT_VALUES_EQUAL(
            metadataServerAddress,
            metadataServerConfig.GetEndpoint());
    }

    Y_UNIT_TEST(ShouldSetupIamMetadataServerConfig)
    {
        const TString endpoint = "localhost:123";

        NProto::TLogbrokerConfig proto;
        auto& metadataServerProto = *proto.MutableIamMetadataServer();
        metadataServerProto.SetEndpoint(endpoint);

        TLogbrokerConfig config{proto};
        UNIT_ASSERT_VALUES_EQUAL("", config.GetMetadataServerAddress());

        auto auth = config.GetAuthConfig();

        auto& metadataServerConfig = std::get<TIamMetadataServer>(auth);

        UNIT_ASSERT_VALUES_EQUAL(endpoint, metadataServerConfig.GetEndpoint());
    }

    Y_UNIT_TEST(ShouldSetupIamJwtFile)
    {
        const TString endpoint = "localhost:123";
        const TString path = "path";

        NProto::TLogbrokerConfig proto;
        auto& iamJwtFileProto = *proto.MutableIamJwtFile();
        iamJwtFileProto.SetIamEndpoint(endpoint);
        iamJwtFileProto.SetJwtFilename(path);

        TLogbrokerConfig config{proto};
        UNIT_ASSERT_VALUES_EQUAL("", config.GetMetadataServerAddress());

        auto auth = config.GetAuthConfig();

        auto& iamJwtFileConfig = std::get<TIamJwtFile>(auth);

        UNIT_ASSERT_VALUES_EQUAL(endpoint, iamJwtFileConfig.GetIamEndpoint());
        UNIT_ASSERT_VALUES_EQUAL(path, iamJwtFileConfig.GetJwtFilename());
    }
}

}   // namespace NCloud::NBlockStore::NLogbroker
