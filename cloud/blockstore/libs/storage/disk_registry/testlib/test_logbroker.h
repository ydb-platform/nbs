#pragma once

#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/public/api/protos/disk.pb.h>

#include <util/generic/deque.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest {

////////////////////////////////////////////////////////////////////////////////

class TTestLogbrokerService: public NLogbroker::IService
{
public:
    struct TItem
    {
        TString DiskId;
        NProto::EDiskState State;
        TString Message;
        ui64 SeqNo;
    };

private:
    const NProto::TError Error;

    TMutex ItemsMtx;
    TDeque<TItem> Items;

public:
    TTestLogbrokerService() = default;
    explicit TTestLogbrokerService(NProto::TError error)
        : Error(error)
    {}

    NThreading::TFuture<NProto::TError> Write(
        TVector<NLogbroker::TMessage> messages,
        TInstant now) override;

    void Start() override;
    void Stop() override;

    size_t GetItemCount() const;
    TDeque<TItem> ExtractItems();
};

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest
