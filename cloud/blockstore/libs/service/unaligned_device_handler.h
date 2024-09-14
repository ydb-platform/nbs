#pragma once

#include "aligned_device_handler.h"

#include <cloud/blockstore/libs/service/device_handler.h>

#include <util/generic/list.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////
class TModifyRequest;
using TModifyRequestPtr = std::shared_ptr<TModifyRequest>;
using TModifyRequestWeakPtr = std::weak_ptr<TModifyRequest>;
using TModifyRequestIt = TList<TModifyRequestPtr>::iterator;

class TModifyRequest: public std::enable_shared_from_this<TModifyRequest>
{
private:
    enum class EStatus
    {
        Waiting,
        InFlight,
    };
    TVector<TModifyRequestWeakPtr> Dependencies;
    TModifyRequestIt It;
    EStatus Status = EStatus::Waiting;

protected:
    const std::weak_ptr<TAlignedDeviceHandler> Backend;
    const TBlocksInfo BlocksInfo;
    TCallContextPtr CallContext;
    TStorageBuffer RMWBuffer;
    TSgList RMWBufferSgList;

public:
    TModifyRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo);

    virtual ~TModifyRequest() = default;

    void SetIt(TModifyRequestIt it);
    TModifyRequestIt GetIt() const;

    bool IsAligned() const;
    void AddDependencies(const TList<TModifyRequestPtr>& requests);

    //
    [[nodiscard]] bool ReadyToRun();

    void Postpone();
    void ExecutePostponed();

protected:
    void AllocateRMWBuffer();

    virtual void DoPostpone() = 0;
    virtual void DoExecutePostponed() = 0;
};

class TWriteRequest final: public TModifyRequest
{
public:
    using TResponsePromise =
        NThreading::TPromise<NProto::TWriteBlocksLocalResponse>;
    using TResponseFuture =
        NThreading::TFuture<NProto::TWriteBlocksLocalResponse>;

private:
    TResponsePromise Promise;
    TGuardedSgList SgList;

public:
    TWriteRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo,
        TGuardedSgList sgList);

    TResponseFuture ExecuteOrPostpone(bool readyToRun);

protected:
    void DoPostpone() override;
    void DoExecutePostponed() override;

private:
    TResponseFuture DoExecute();
    TResponseFuture ReadModifyWrite(TAlignedDeviceHandler* backend);
    TResponseFuture ModifyAndWrite();
};

class TZeroRequest final: public TModifyRequest
{
public:
    using TResponsePromise = NThreading::TPromise<NProto::TZeroBlocksResponse>;
    using TResponseFuture = NThreading::TFuture<NProto::TZeroBlocksResponse>;

private:
    TResponsePromise Promise;
    TGuardedSgList SgList;

public:
    TZeroRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo);
    ~TZeroRequest() override;

    TResponseFuture ExecuteOrPostpone(bool readyToRun);

protected:
    void DoPostpone() override;
    void DoExecutePostponed() override;

private:
    TResponseFuture DoExecute();
    TResponseFuture ReadModifyWrite(TAlignedDeviceHandler* backend);
    TResponseFuture ModifyAndWrite();
};

class TUnalignedDeviceHandler
    : public IDeviceHandler
    , public std::enable_shared_from_this<TUnalignedDeviceHandler>
{
private:
    const std::shared_ptr<TAlignedDeviceHandler> Backend;
    const ui32 BlockSize;
    const ui32 MaxUnalignedBlockCount;

    TList<TModifyRequestPtr> AlignedRequests;
    TList<TModifyRequestPtr> UnalignedRequests;
    TAdaptiveLock RequestsLock;

public:
    TUnalignedDeviceHandler(
        IStoragePtr storage,
        TString clientId,
        ui32 blockSize,
        ui32 maxBlockCount);

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> Read(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) override;

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> Write(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) override;

    NThreading::TFuture<NProto::TZeroBlocksResponse>
    Zero(TCallContextPtr ctx, ui64 from, ui64 length) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

private:
    [[nodiscard]] bool RegisterRequest(TModifyRequestPtr request);

    NThreading::TFuture<NProto::TReadBlocksResponse>
    ExecuteUnalignedReadRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList,
        TString checkpointId) const;

    void OnRequestFinished(TModifyRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
