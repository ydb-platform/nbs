#include "helpers.h"

#include <cloud/filestore/config/client.pb.h>
#include <cloud/filestore/public/api/protos/endpoint.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEndpointVhostHelpersTest)
{
    Y_UNIT_TEST(ShouldConvertEndpointConfigToSessionConfig)
    {
        {
            NProto::TEndpointConfig endpointConfig;

            NProto::TSessionConfig sessionConfig;
            UNIT_ASSERT(!sessionConfig.HasFileSystemId());
            UNIT_ASSERT(!sessionConfig.HasClientId());
            UNIT_ASSERT(!sessionConfig.HasSessionPingTimeout());

            EndpointConfigToSessionConfig(endpointConfig, &sessionConfig);
            UNIT_ASSERT(sessionConfig.HasFileSystemId());
            UNIT_ASSERT(sessionConfig.HasClientId());
            UNIT_ASSERT(!sessionConfig.HasSessionPingTimeout());
            UNIT_ASSERT_VALUES_EQUAL("", sessionConfig.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL("", sessionConfig.GetClientId());
        }

        {
            NProto::TEndpointConfig endpointConfig;
            endpointConfig.SetFileSystemId("fs0");
            endpointConfig.SetClientId("c0");

            NProto::TSessionConfig sessionConfig;

            EndpointConfigToSessionConfig(endpointConfig, &sessionConfig);
            UNIT_ASSERT(sessionConfig.HasFileSystemId());
            UNIT_ASSERT(sessionConfig.HasClientId());
            UNIT_ASSERT(!sessionConfig.HasSessionPingTimeout());
            UNIT_ASSERT_VALUES_EQUAL("fs0", sessionConfig.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL("c0", sessionConfig.GetClientId());

            endpointConfig.SetSessionPingTimeout(0);
            EndpointConfigToSessionConfig(endpointConfig, &sessionConfig);
            UNIT_ASSERT(!sessionConfig.HasSessionPingTimeout());

            endpointConfig.SetSessionPingTimeout(5);
            EndpointConfigToSessionConfig(endpointConfig, &sessionConfig);
            UNIT_ASSERT(sessionConfig.HasSessionPingTimeout());
            UNIT_ASSERT_VALUES_EQUAL(5, sessionConfig.GetSessionPingTimeout());
        }
    }
}

}   // namespace NCloud::NFileStore::NServer
