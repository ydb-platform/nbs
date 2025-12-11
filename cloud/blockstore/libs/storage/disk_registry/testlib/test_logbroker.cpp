#include "test_logbroker.h"

namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TTestLogbrokerService::Write(
    TVector<NLogbroker::TMessage> messages,
    TInstant now)
{
    Y_UNUSED(now);

    for (auto& [payload, seqNo]: messages) {
        NProto::TDiskState state;
        const bool ok = state.ParseFromArray(payload.data(), payload.size());

        Y_ABORT_UNLESS(ok);

        with_lock (ItemsMtx) {
            Items.push_back(TItem{
                .DiskId = state.GetDiskId(),
                .State = state.GetState(),
                .Message = state.GetStateMessage(),
                .SeqNo = seqNo});
        }
    }

    return MakeFuture(Error);
}

void TTestLogbrokerService::Start()
{}

void TTestLogbrokerService::Stop()
{}

size_t TTestLogbrokerService::GetItemCount() const
{
    with_lock (ItemsMtx) {
        return Items.size();
    }
}

auto TTestLogbrokerService::ExtractItems() -> TDeque<TItem>
{
    TDeque<TItem> tmp;

    with_lock (ItemsMtx) {
        Items.swap(tmp);
    }

    return tmp;
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest
