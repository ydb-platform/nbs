#include "opaque_config_parser.h"

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/yaml_config/yaml_config_helpers.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

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

TResultOrError<NProto::TBlockstoreConfig> ExtractBlockstoreConfig(
    const google::protobuf::Message& privateDatabaseConfig)
{
    // Preserve parser diagnostics while classifying every TError as a failure.
    const auto* descriptor = privateDatabaseConfig.GetDescriptor();
    if (descriptor == NCloud::NProto::TError::descriptor()) {
        NCloud::NProto::TError error;
        error.CopyFrom(privateDatabaseConfig);
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Failed to parse PrivateDatabaseConfig from CMS: "
                << FormatError(error));
    }

    // Reject unexpected types without including their configuration values.
    if (descriptor != NProto::TBlockstoreConfig::descriptor()) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Internal error: received an unexpected "
                   "PrivateDatabaseConfig payload type "
                << descriptor->full_name() << "; expected "
                << NProto::TBlockstoreConfig::descriptor()->full_name());
    }

    // Copy the payload so callers can normalize it without changing the source.
    NProto::TBlockstoreConfig config;
    config.CopyFrom(privateDatabaseConfig);
    return config;
}

}   // namespace NCloud::NBlockStore
