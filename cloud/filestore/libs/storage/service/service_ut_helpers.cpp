#include "service_ut_helpers.h"

#include <cloud/filestore/libs/storage/tablet/events/tablet_private.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

void TTestProfileLog::Start()
{}

void TTestProfileLog::Stop()
{}

void TTestProfileLog::Write(TRecord record)
{
    UNIT_ASSERT(record.Request.HasRequestType());
    Requests[record.Request.GetRequestType()].push_back(std::move(record));
}

void TTestProfileLog::RegisterCounters(NMonitoring::TDynamicCounters& root)
{
    Y_UNUSED(root);
}

TString GenerateValidateData(ui32 size, ui32 seed)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

void WaitForTabletStart(TServiceClient& service)
{
    TDispatchOptions options;
    options.FinalEvents = {
        TDispatchOptions::TFinalEventCondition(
            TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest)};
    service.AccessRuntime().DispatchEvents(options, TDuration::Seconds(5));
}

}   // namespace NCloud::NFileStore::NStorage
