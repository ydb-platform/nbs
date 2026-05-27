#include "protobuf_utils.h"

#include <cloud/filestore/libs/storage/service/service_ut_helpers.h>

#include "cloud/storage/core/libs/common/error.h"
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TString> CreateIovecs(size_t num, size_t size)
{
    TVector<TString> iovecs;
    iovecs.reserve(num);
    for (size_t i = 0; i < num; ++i) {
        iovecs.emplace_back(size, '\0');
    }
    return iovecs;
}

////////////////////////////////////////////////////////////////////////////////

TString GetBufferFromIovecs(const TVector<TString>& iovecs, size_t length)
{
    size_t bufOffset = 0;
    TString buf;
    buf.resize(length);

    for (const auto& iovec: iovecs) {
        if (length == 0) {
            break;
        }

        size_t len = std::min(iovec.size(), length);
        memcpy(&buf[bufOffset], iovec.data(), len);
        length -= len;
        bufOffset += len;
    }

    return buf;
}

////////////////////////////////////////////////////////////////////////////////

void FillIovecs(
    ::google::protobuf::RepeatedPtrField<NProto::TIovec>& protoIovecs,
    const TVector<TString>& iovecs)
{
    for (const auto& buf: iovecs) {
        auto* iovec = protoIovecs.Add();
        iovec->SetBase(reinterpret_cast<ui64>(buf.data()));
        iovec->SetLength(buf.size());
    }
}

////////////////////////////////////////////////////////////////////////////////

auto TestParseReadDataResponse(
    TString input,
    NProto::TReadDataResponse& outputResponse,
    ::google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs)
{
    TRope rope(std::move(input));

    NActors::TEventSerializedData serializedData(
        std::move(rope),
        NActors::TEventSerializationInfo{});

    return ParseReadDataResponse(serializedData, outputResponse, iovecs);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProtobufUtilsTest)
{
    Y_UNIT_TEST(ShouldParseEmptyReadDataResponse)
    {
        NProto::TReadDataResponse expectedResponse;
        NProto::TReadDataResponse actualResponse;
        ::google::protobuf::RepeatedPtrField<NProto::TIovec> iovecs;
        auto ret = TestParseReadDataResponse(
            expectedResponse.SerializeAsString(),
            actualResponse,
            iovecs);
        UNIT_ASSERT(!HasError(ret));
        UNIT_ASSERT_VALUES_EQUAL(
            expectedResponse.SerializeAsString(),
            actualResponse.SerializeAsString());
    }

    Y_UNIT_TEST(ShouldParseErrorReadDataResponse)
    {
        NProto::TReadDataResponse expectedResponse;
        NProto::TReadDataResponse actualResponse;
        ::google::protobuf::RepeatedPtrField<NProto::TIovec> iovecs;
        auto* err = expectedResponse.MutableError();
        err->SetCode(E_FAIL);
        err->SetMessage("error message");
        auto ret = TestParseReadDataResponse(
            expectedResponse.SerializeAsString(),
            actualResponse,
            iovecs);
        UNIT_ASSERT(!HasError(ret));
        UNIT_ASSERT_VALUES_EQUAL(
            expectedResponse.SerializeAsString(),
            actualResponse.SerializeAsString());
    }

    Y_UNIT_TEST(ShouldParseReadDataResponse)
    {
        NProto::TReadDataRequest request;
        NProto::TReadDataResponse expectedResponse;
        TString buffer = GenerateValidateData(1_MB, 0);
        auto iovecs = CreateIovecs(1_MB / 4_KB, 4_KB);
        FillIovecs(*request.MutableIovecs(), iovecs);
        expectedResponse.SetBuffer(std::move(buffer));
        expectedResponse.SetBufferOffset(0);
        expectedResponse.SetLength(0);
        expectedResponse.MutableHeaders()
            ->MutableBackendInfo()
            ->SetIsOverloaded(true);

        NProto::TReadDataResponse actualResponse;

        auto ret = TestParseReadDataResponse(
            expectedResponse.SerializeAsString(),
            actualResponse,
            *request.MutableIovecs());
        UNIT_ASSERT(!HasError(ret));
        UNIT_ASSERT_VALUES_EQUAL(1_MB, actualResponse.GetLength());

        auto actualBuffer =
            GetBufferFromIovecs(iovecs, actualResponse.GetLength());
        UNIT_ASSERT_VALUES_EQUAL(expectedResponse.GetBuffer(), actualBuffer);
    }

      Y_UNIT_TEST(ShouldFailOnNonZeroBufferOffset)
    {
        NProto::TReadDataRequest request;
        NProto::TReadDataResponse expectedResponse;
        TString buffer = GenerateValidateData(1_MB, 0);
        auto iovecs = CreateIovecs(1_MB / 4_KB, 4_KB);
        FillIovecs(*request.MutableIovecs(), iovecs);
        expectedResponse.SetBuffer(std::move(buffer));
        expectedResponse.SetBufferOffset(100);

        NProto::TReadDataResponse actualResponse;

        auto ret = TestParseReadDataResponse(
            expectedResponse.SerializeAsString(),
            actualResponse,
            *request.MutableIovecs());
        UNIT_ASSERT(HasError(ret));
    }

    Y_UNIT_TEST(ShouldFailOnNonZeroLength)
    {
        NProto::TReadDataRequest request;
        NProto::TReadDataResponse expectedResponse;
        TString buffer = GenerateValidateData(1_MB, 0);
        auto iovecs = CreateIovecs(1_MB / 4_KB, 4_KB);
        FillIovecs(*request.MutableIovecs(), iovecs);
        expectedResponse.SetBuffer(std::move(buffer));
        expectedResponse.SetLength(expectedResponse.GetBuffer().size());

        NProto::TReadDataResponse actualResponse;

        auto ret = TestParseReadDataResponse(
            expectedResponse.SerializeAsString(),
            actualResponse,
            *request.MutableIovecs());
        UNIT_ASSERT(HasError(ret));
    }

    Y_UNIT_TEST(ShouldNotCrash)
    {
        ILoggingServicePtr Logging(
            CreateLoggingService("console", {TLOG_DEBUG}));
        TLog Log(Logging->CreateLog("NFS_TEST"));
        const auto seed = time(0);
        STORAGE_INFO("Seed: %lu", seed);
        srand(seed);

        NProto::TReadDataRequest request;
        NProto::TReadDataResponse expectedResponse;
        TString buffer = GenerateValidateData(1_MB, seed);
        auto iovecs = CreateIovecs(1_MB / 4_KB, 4_KB);
        FillIovecs(*request.MutableIovecs(), iovecs);
        expectedResponse.SetBuffer(std::move(buffer));
        expectedResponse.SetBufferOffset(100);
        expectedResponse.MutableHeaders()
            ->MutableBackendInfo()
            ->SetIsOverloaded(true);

        // corrupt the serialized response
        auto serializedData = expectedResponse.SerializeAsString();

        auto mutationCount = rand() % 10;
        for (auto i = 0; i < mutationCount; ++i) {
            size_t mutationLen = rand() % 20;
            size_t mutationPos = rand() % serializedData.size();

            for (size_t i = mutationPos;
                 i < std::min(mutationPos + mutationLen, serializedData.size());
                 ++i)
            {
                serializedData[i] = rand() % 256;
            }
        }

        NProto::TReadDataResponse actualResponse;
        TestParseReadDataResponse(
            std::move(serializedData),
            actualResponse,
            *request.MutableIovecs());
    }
}

}   // namespace NCloud::NFileStore::NStorage
