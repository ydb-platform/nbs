#include <cloud/blockstore/libs/config/opaque_config_parser.h>

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockstoreOpaqueConfigParserTest)
{
    // Check valid YAML conversion and rejection of an invalid section value.
    Y_UNIT_TEST(ShouldUseDefaultOpaqueConfigParser)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        const auto message = parser(R"(
storage_service:
  write_blob_threshold: 300
)");

        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::TBlockstoreConfig::descriptor(),
            message->GetDescriptor());

        NProto::TBlockstoreConfig config;
        config.CopyFrom(*message);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            config.GetStorageService().GetWriteBlobThreshold());

        const auto result = parser("storage_service: invalid");
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            NCloud::NProto::TError::descriptor(),
            result->GetDescriptor());

        // Preserve the conversion failure details for the caller's log.
        NCloud::NProto::TError error;
        error.CopyFrom(*result);
        UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "expected json map");
    }

    // Check that invalid inputs return errors with diagnostic messages.
    Y_UNIT_TEST(ShouldReturnErrorsForInvalidOpaqueConfig)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        for (const TString yaml: {
                 "storage_service: [",
                 "storage_service: private-secret-value"})
        {
            const auto message = parser(yaml);
            UNIT_ASSERT(message);
            UNIT_ASSERT_C(
                NCloud::NProto::TError::descriptor() == message->GetDescriptor(),
                yaml);

            NCloud::NProto::TError error;
            error.CopyFrom(*message);
            UNIT_ASSERT(FAILED(error.GetCode()));
            UNIT_ASSERT(!error.GetMessage().empty());
        }
    }

    // Check that a null repeated message returns an error for its required fields.
    Y_UNIT_TEST(ShouldRejectMissingRequiredFields)
    {
        // Parse a repeated element that creates an interval without boundaries.
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        const auto message = parser(R"(
diagnostics:
  execution_time_size_classes: [null]
)");

        // Return an input error with both missing fields instead of a config.
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(
            NCloud::NProto::TError::descriptor(),
            message->GetDescriptor());
        NCloud::NProto::TError error;
        error.CopyFrom(*message);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            error.GetMessage(),
            "Diagnostics.ExecutionTimeSizeClasses[0].Start");
        UNIT_ASSERT_STRING_CONTAINS(
            error.GetMessage(),
            "Diagnostics.ExecutionTimeSizeClasses[0].End");
    }

    // Check that optional fields may be omitted while present intervals are valid.
    Y_UNIT_TEST(ShouldAcceptPartialConfigWithInitializedMessages)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        for (const TString yaml: {
                 "storage_service: {write_blob_threshold: 300}",
                 "diagnostics: {}",
                 "diagnostics: {execution_time_size_classes: []}",
                 "diagnostics: {execution_time_size_classes: "
                 "[{start: 0, end: 4096}]}"})
        {
            const auto message = parser(yaml);
            UNIT_ASSERT(message);
            UNIT_ASSERT_C(
                NProto::TBlockstoreConfig::descriptor() ==
                    message->GetDescriptor(),
                yaml);
            UNIT_ASSERT_C(message->IsInitialized(), yaml);
        }
    }

    // Check that empty input, empty mappings, and unknown fields produce
    // empty configs.
    Y_UNIT_TEST(ShouldAcceptEmptyAndUnknownOpaqueConfig)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        for (const TString yaml: {
                 "",
                 " \t\n",
                 "---\n",
                 "{}",
                 "null",
                 "future_option: 1"})
        {
            const auto message = parser(yaml);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::TBlockstoreConfig::descriptor(),
                message->GetDescriptor());
            UNIT_ASSERT_VALUES_EQUAL(0, message->ByteSizeLong());
        }
    }

    // Check that parser errors retain diagnostics and remain distinguishable
    // from unexpected payload types regardless of the original error code.
    Y_UNIT_TEST(ShouldDistinguishParserErrorsFromUnexpectedPayloads)
    {
        // Classify a parser error independently of the code it carries.
        const auto parseError =
            MakeError(E_INVALID_STATE, "parser diagnostics");
        const auto result = ExtractBlockstoreConfig(parseError);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            result.GetError().GetMessage(),
            FormatError(parseError));

        // Reject even a success-coded TError instead of accepting an empty
        // config.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ExtractBlockstoreConfig(NCloud::NProto::TError())
                .GetError()
                .GetCode());

        // Identify an unexpected type without exposing its field values.
        NProto::TStorageServiceConfig unexpectedConfig;
        unexpectedConfig.SetNodeType("private-secret-value");
        const auto unexpectedResult = ExtractBlockstoreConfig(unexpectedConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            unexpectedResult.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            unexpectedResult.GetError().GetMessage(),
            NProto::TStorageServiceConfig::descriptor()->full_name());
        UNIT_ASSERT_STRING_CONTAINS(
            unexpectedResult.GetError().GetMessage(),
            NProto::TBlockstoreConfig::descriptor()->full_name());
        UNIT_ASSERT(!unexpectedResult.GetError().GetMessage().Contains(
            "private-secret-value"));
    }
}

}   // namespace NCloud::NBlockStore
