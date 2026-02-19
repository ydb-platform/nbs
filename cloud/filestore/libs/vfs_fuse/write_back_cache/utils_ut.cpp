#include "utils.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUtilsTest)
{
    Y_UNIT_TEST(ShouldValidateReadDataRequests)
    {
        const TString FileSystemId = "fs_id";

        {
            // Invalid FileSystemId
            auto rq = std::make_shared<NProto::TReadDataRequest>();
            rq->SetFileSystemId("fs_id_bad");
            rq->SetLength(1);

            auto e = TUtils::ValidateReadDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, e.GetCode());
        }

        {
            // Incorrect length
            auto rq = std::make_shared<NProto::TReadDataRequest>();
            rq->SetFileSystemId(FileSystemId);
            rq->SetLength(0);

            auto e = TUtils::ValidateReadDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, e.GetCode());
        }

        {
            // Normal request
            auto rq = std::make_shared<NProto::TReadDataRequest>();
            rq->SetFileSystemId(FileSystemId);
            rq->SetLength(1);

            auto e = TUtils::ValidateReadDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, e.GetCode());
        }
    }

    Y_UNIT_TEST(ShouldValidateWriteDataRequests)
    {
        const TString FileSystemId = "fs_id";

        {
            // No buffer and iovecs
            auto rq = std::make_shared<NProto::TWriteDataRequest>();
            rq->SetFileSystemId(FileSystemId);

            auto e = TUtils::ValidateWriteDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, e.GetCode());
        }

        {
            // Invalid buffer offset
            auto rq = std::make_shared<NProto::TWriteDataRequest>();
            rq->SetFileSystemId(FileSystemId);
            rq->SetBuffer("abc");
            rq->SetBufferOffset(3);

            auto e = TUtils::ValidateWriteDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, e.GetCode());
        }

        {
            // Both buffer and iovecs
            auto rq = std::make_shared<NProto::TWriteDataRequest>();
            rq->SetFileSystemId(FileSystemId);
            rq->SetBuffer("abc");
            rq->AddIovecs()->SetBase(0);
            rq->AddIovecs()->SetLength(2);

            auto e = TUtils::ValidateWriteDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, e.GetCode());
        }

        {
            // Invalid iovec length
            auto rq = std::make_shared<NProto::TWriteDataRequest>();
            rq->SetFileSystemId(FileSystemId);
            auto* iovec = rq->AddIovecs();
            iovec->SetBase(0);
            iovec->SetLength(0);

            auto e = TUtils::ValidateWriteDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, e.GetCode());
        }

        {
            // Invalid FileSystemId
            auto rq = std::make_shared<NProto::TWriteDataRequest>();
            rq->SetFileSystemId("fs_id_bad");
            rq->SetBuffer("123");

            auto e = TUtils::ValidateWriteDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, e.GetCode());
        }

        {
            // Normal request with buffer
            auto rq = std::make_shared<NProto::TWriteDataRequest>();
            rq->SetFileSystemId(FileSystemId);
            rq->SetBuffer("123");

            auto e = TUtils::ValidateWriteDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, e.GetCode());
        }

        {
            // Normal request with iovecs
            auto rq = std::make_shared<NProto::TWriteDataRequest>();
            rq->SetFileSystemId(FileSystemId);
            auto* iovec = rq->AddIovecs();
            iovec->SetBase(0);
            iovec->SetLength(1);

            auto e = TUtils::ValidateWriteDataRequest(*rq, FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, e.GetCode());
        }
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
