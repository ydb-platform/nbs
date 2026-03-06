#include "service_ut_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

void TTestProfileLog::Start()
{}

void TTestProfileLog::Stop()
{}

void TTestProfileLog::Write(TRecord record)
{
    UNIT_ASSERT(record.Request.HasRequestType());
    Requests[record.Request.GetRequestType()].push_back(std::move(record));
}

TString GenerateValidateData(ui32 size, ui32 seed)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

}   // namespace NCloud::NFileStore::NStorage
