#include "device.h"

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TInMemoryDevice final: public IDevice
{
private:
    TAdaptiveLock Lock;
    THashMap<ui64 /*pageNo*/, TString> Pages;

public:
    TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        NCloud::NProto::TReadPagesResponse response;

        with_lock (Lock) {
            for (const auto& ref: request.GetPageGroupRefs()) {
                auto& group = *response.AddPageGroups();
                group.SetFirstPageNo(ref.GetFirstPageNo());

                for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
                    auto it = Pages.find(ref.GetFirstPageNo() + i);
                    if (it != Pages.end()) {
                        *group.AddContent() = it->second;
                        continue;
                    }

                    // a page that has never been written reads as a zeroed one
                    *group.AddContent() = TString(ref.GetPageSize(), '\0');
                }
            }
        }

        return MakeFuture(std::move(response));
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> WritePages(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        with_lock (Lock) {
            for (auto& group: *request.MutablePageGroups()) {
                ui64 pageNo = group.GetFirstPageNo();
                for (auto& content: *group.MutableContent()) {
                    Pages[pageNo++] = std::move(content);
                }
            }
        }

        return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateInMemoryDevice()
{
    return std::make_shared<TInMemoryDevice>();
}

}   // namespace NCloud::NJournalled
