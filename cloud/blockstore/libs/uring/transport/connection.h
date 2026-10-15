#pragma once

#include "public.h"

#include "protocol.h"
#include "ring.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>
#include <memory>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionParams
{
    ui32 MaxProtoSize = 0;
    ui32 MaxDataSize = 0;
    ui32 SendWindowBytes = 0;
    bool OobEnabled = false;
    ui32 OobThreshold = 0;
    bool SendZeroCopy = false;
    ui32 SendZeroCopyThreshold = 0;
};

// Head: TFrameHeader followed by proto bytes. The caller fills Type,
// RequestId and ProtoLen; the connection fills Flags and DataLen and takes
// Head over. OnDone runs when the kernel no longer references Head/Data.
struct TOutFrame
{
    TString Head;
    TSgList Data;
    std::function<void()> OnDone;

    ~TOutFrame()
    {
        if (OnDone) {
            OnDone();
        }
    }
};

using TOutFramePtr = std::shared_ptr<TOutFrame>;

// Where an OOB payload goes. Empty Target — read and discard. A non-empty
// Target must cover exactly DataLen bytes.
struct TDataSink
{
    TSgList Target;
    std::function<void(const NProto::TError&)> OnDone;
};

struct IConnectionHandler
{
    virtual ~IConnectionHandler() = default;

    virtual void OnFrame(
        const TFrameHeader& header,
        TStringBuf proto,
        TStringBuf inlineData) = 0;

    virtual TDataSink OnOobFrame(
        const TFrameHeader& header,
        TStringBuf proto) = 0;

    virtual void OnConnectionClosed(const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// A pair of TCP streams: the main one carries frame headers, proto and small
// inline payloads, the data one carries large payloads out of band. Both are
// driven by one receive and one send operation. All methods, the destructor
// included, run on the ring thread only. The owner must keep the connection
// alive until OnConnectionClosed has been delivered; it may destroy it from
// inside that callback.
class TConnection final: public IIoHandler
{
    enum ETag : ui32
    {
        TAG_MAIN_RECV = 1,
        TAG_MAIN_SEND = 2,
        TAG_DATA_RECV = 3,
        TAG_DATA_SEND = 4,
    };

    // One frame's bytes on one stream: Head is the frame header and proto on
    // the main stream, the data header on the data stream. Offset counts the
    // bytes the kernel has taken, across Head and Data. Shared, because a
    // chunk has to outlive every op whose iovecs point into it, and a partial
    // zero-copy send leaves two of them pointing at the same chunk.
    struct TChunk
    {
        TString Head;
        TSgList Data;
        size_t Total = 0;
        size_t Offset = 0;
        TOutFramePtr Frame;
    };

    using TChunkPtr = std::shared_ptr<TChunk>;

    struct TStream
    {
        int Fd = -1;
        ui32 SendTag = 0;
        TDeque<TChunkPtr> Queue;
        TIoOp* SendOp = nullptr;
    };

    enum class EDataState
    {
        Idle,
        ReadingHeader,
        WaitingAdmission,
        ReadingPayload,
    };

    struct TExpect
    {
        ui64 RequestId = 0;
        ui32 DataLen = 0;
        TDataSink Sink;
        size_t Done = 0;
        size_t RefIndex = 0;
        size_t RefOffset = 0;
    };

private:
    TRing& Ring;
    const TConnectionParams Params;
    IConnectionHandler* Handler;
    TLog Log;

    TStream Main;
    TStream Data;

    TIoOp* MainRecvOp = nullptr;
    TString RecvBuf;
    size_t Filled = 0;
    size_t Parsed = 0;

    TIoOp* DataRecvOp = nullptr;
    EDataState DataState = EDataState::Idle;
    TDataHeader DataHdr;
    size_t DataHdrDone = 0;
    TDeque<TExpect> DataFifo;
    TString Scratch;

    // Send ops the kernel still owns, each holding the chunks its iovecs point
    // into: a plain send releases them on completion, a zero-copy one only on
    // its notification.
    THashMap<TIoOp*, TVector<TChunkPtr>> InFlightSends;

    bool Closing = false;
    bool Finished = false;
    NProto::TError CloseError;

public:
    TConnection(
        TRing& ring,
        TConnectionParams params,
        int mainFd,
        int dataFd,
        IConnectionHandler* handler,
        TLog log);
    ~TConnection() override;

    void Start();
    void Send(TOutFramePtr frame);
    void Close(NProto::TError error);

    bool IsClosing() const
    {
        return Closing;
    }

private:
    void OnIoComplete(TIoOp* op, int res, ui32 cqeFlags) override;

    void StartMainRecv();
    void OnMainRecv(int res);
    bool ParseFrames();
    void Compact(size_t needed);

    void StartDataHeaderRecv();
    void OnDataRecv(int res);
    void TryProgressData();
    void IssuePayloadRecv();
    void FinishPayload();

    void StartSend(TStream& stream);
    void OnSendComplete(TStream& stream, TIoOp* op, int res, ui32 cqeFlags);

    void MaybeFinishClose();
    void ProtocolError(TStringBuf what);
};

}   // namespace NCloud::NBlockStore::NUring
