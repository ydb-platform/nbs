#include "opaque_config_parser.h"

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/yaml_config/yaml_config_helpers.h>

#include <util/generic/yexception.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Parse PrivateDatabaseConfig, accepting empty input and preserving error
// details.
std::shared_ptr<const google::protobuf::Message> BlockstoreOpaqueConfigParser(
    const TString& opaqueYamlConfig)
{
    try {
        auto config = NKikimr::NYaml::DefaultOpaqueConfigParser<
            NProto::TBlockstoreConfig>(
            opaqueYamlConfig,
            /*allowUnknownFields=*/true);
        if (config) {
            Y_ENSURE(
                config->IsInitialized(),
                config->InitializationErrorString());
            return config;
        }

        // Return a non-null message for successful parsing of empty input.
        return std::make_shared<NProto::TBlockstoreConfig>();
    } catch (...) {
        return std::make_shared<NCloud::NProto::TError>(
            MakeError(E_ARGUMENT, CurrentExceptionMessage()));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NKikimr::NConfig::TOpaqueConfigParser CreateBlockstoreOpaqueConfigParser()
{
    return BlockstoreOpaqueConfigParser;
}

}   // namespace NCloud::NBlockStore
