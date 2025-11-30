#include "dataset_output.h"

#include <cloud/blockstore/libs/service/request.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration TestOffset = TDuration::Seconds(2);

Y_UNIT_TEST_SUITE(TDatasetOutputTest)
{
    Y_UNIT_TEST(Test)
    {
        TDatasetOutput datasetOutput("/home/komarevtsev/test.txt");

        NProto::TProfileLogRecord record;
        record.SetDiskId("test");
        {
            auto* request = record.AddRequests();
            request->SetTimestampMcs(100);
            request->SetRequestType(
                static_cast<ui32>(EBlockStoreRequest::ReadBlocks));
            request->SetDurationMcs(100);
            request->SetPostponedTimeMcs(1);
            auto* range = request->AddRanges();
            range->SetBlockCount(1);
            range->SetBlockIndex(777);
        }

        {
            auto* request = record.AddRequests();
            request->SetTimestampMcs(1000 + TestOffset.MicroSeconds());
            request->SetRequestType(
                static_cast<ui32>(EBlockStoreRequest::ReadBlocks));
            request->SetDurationMcs(500);
            request->SetPostponedTimeMcs(100);
            auto* range = request->AddRanges();
            range->SetBlockCount(1);
            range->SetBlockIndex(777);
        }

        {
            auto* request = record.AddRequests();
            request->SetTimestampMcs(1000 + TestOffset.MicroSeconds());
            request->SetRequestType(
                static_cast<ui32>(EBlockStoreRequest::ReadBlocks));
            request->SetDurationMcs(400);
            request->SetPostponedTimeMcs(100);
            auto* range = request->AddRanges();
            range->SetBlockCount(1);
            range->SetBlockIndex(777);
        }

        {
            auto* request = record.AddRequests();
            request->SetTimestampMcs(600 + TestOffset.MicroSeconds());
            request->SetRequestType(
                static_cast<ui32>(EBlockStoreRequest::ReadBlocks));
            request->SetDurationMcs(400);
            request->SetPostponedTimeMcs(100);
            auto* range = request->AddRanges();
            range->SetBlockCount(1);
            range->SetBlockIndex(777);
        }

        {
            auto* request = record.AddRequests();
            request->SetTimestampMcs(800 + TestOffset.MicroSeconds());
            request->SetRequestType(
                static_cast<ui32>(EBlockStoreRequest::ReadBlocks));
            request->SetDurationMcs(400);
            request->SetPostponedTimeMcs(100);
            auto* range = request->AddRanges();
            range->SetBlockCount(1);
            range->SetBlockIndex(777);
        }

        {
            auto* request = record.AddRequests();
            request->SetTimestampMcs(700 + TestOffset.MicroSeconds());
            request->SetRequestType(
                static_cast<ui32>(EBlockStoreRequest::ReadBlocks));
            request->SetDurationMcs(100);
            request->SetPostponedTimeMcs(0);
            auto* range = request->AddRanges();
            range->SetBlockCount(1);
            range->SetBlockIndex(777);
        }

        datasetOutput.ProcessRequests(record);
    }
}

}   // namespace NCloud::NBlockStore
