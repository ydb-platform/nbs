#include "connection.h"

#include "socket.h"

#include <util/generic/utility.h>
#include <util/stream/format.h>
#include <util/string/builder.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace NCloud::NBlockStore::NUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t INITIAL_RECV_BUF = 64 << 10;
constexpr size_t SCRATCH_SIZE = 64 << 10;

void RunOnDone(TOutFrame& frame)
{
    if (frame.OnDone) {
        auto onDone = std::move(frame.OnDone);
        frame.OnDone = nullptr;
        onDone();
    }
}

void Shutdown(int fd)
{
    if (fd >= 0) {
        ::shutdown(fd, SHUT_RDWR);
    }
}

// A zero-copy send holds the pages of the bytes TCP has not delivered yet and
// its notification, which the close waits for, comes only when they are freed.
// Shutting the socket down does not drop them, so cut the user timeout to make
// the kernel give up instead of waiting for a peer that stopped reading.
void DropUndeliveredData(int fd)
{
    if (fd >= 0) {
        const unsigned int timeout = 1;
        ::setsockopt(
            fd,
            IPPROTO_TCP,
            TCP_USER_TIMEOUT,
            &timeout,
            sizeof(timeout));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TConnection::TConnection(
    TRing& ring,
    TConnectionParams params,
    int mainFd,
    int dataFd,
    IConnectionHandler* handler,
    TLog log)
    : Ring(ring)
    , Params(params)
    , Handler(handler)
    , Log(std::move(log))
    , RecvBuf(TString::Uninitialized(INITIAL_RECV_BUF))
{
    Y_ABORT_UNLESS(mainFd >= 0);
    Y_ABORT_UNLESS(
        !params.OobEnabled || dataFd >= 0,
        "OOB is enabled but there is no data socket");
    Main.Fd = mainFd;
    Main.SendTag = TAG_MAIN_SEND;
    Data.Fd = dataFd;
    Data.SendTag = TAG_DATA_SEND;
    MainRecvOp = Ring.AllocOp(this, TAG_MAIN_RECV);
    DataRecvOp = Ring.AllocOp(this, TAG_DATA_RECV);
}

TConnection::~TConnection()
{
    Y_ABORT_UNLESS(Finished || !Closing, "connection destroyed while closing");
    Y_ABORT_UNLESS(!MainRecvOp->InFlight && !DataRecvOp->InFlight);
    Y_ABORT_UNLESS(
        InFlightSends.empty(),
        "connection destroyed with %lu sends in flight",
        InFlightSends.size());
    Ring.FreeOp(MainRecvOp);
    Ring.FreeOp(DataRecvOp);
    if (!Closing) {
        CloseWithReset(Main.Fd);
        CloseWithReset(Data.Fd);
    }
}

void TConnection::Start()
{
    StartMainRecv();
    if (Data.Fd >= 0) {
        StartDataHeaderRecv();
    }
}

////////////////////////////////////////////////////////////////////////////////
// send

void TConnection::Send(TOutFramePtr frame)
{
    Y_DEBUG_ABORT_UNLESS(Ring.IsRingThread());

    if (Closing) {
        // the caller may be waiting on OnDone to release the buffers, and the
        // frame itself may outlive this call
        RunOnDone(*frame);
        return;
    }

    Y_ABORT_UNLESS(frame->Head.size() >= sizeof(TFrameHeader));
    auto* header = reinterpret_cast<TFrameHeader*>(frame->Head.begin());
    Y_ABORT_UNLESS(
        frame->Head.size() == sizeof(TFrameHeader) + header->ProtoLen,
        "frame head %lu does not match ProtoLen %u",
        frame->Head.size(),
        header->ProtoLen);

    // the size of a payload can follow from a request the peer shaped, so an
    // oversized one drops the frame and closes instead of aborting
    const size_t dataLen = SgListGetSize(frame->Data);
    if (dataLen > Params.MaxDataSize) {
        STORAGE_ERROR(
            "payload " << dataLen << " of request " << header->RequestId
                       << " exceeds MaxDataSize " << Params.MaxDataSize);
        RunOnDone(*frame);
        Close(MakeError(
            E_ARGUMENT,
            TStringBuilder() << "payload " << dataLen << " exceeds MaxDataSize "
                             << Params.MaxDataSize));
        return;
    }

    const ui64 requestId = header->RequestId;
    const bool oob =
        dataLen && Params.OobEnabled && dataLen >= Params.OobThreshold;
    header->DataLen = dataLen;
    header->Flags = dataLen ? (oob ? FLAG_OOB : FLAG_INLINE) : 0;

    auto chunk = std::make_shared<TChunk>();
    chunk->Head = std::move(frame->Head);
    if (!oob) {
        chunk->Data = frame->Data;
    }
    chunk->Total = chunk->Head.size() + (oob ? 0 : dataLen);
    chunk->Frame = frame;
    Main.Queue.push_back(std::move(chunk));

    if (oob) {
        TDataHeader dh;
        dh.DataLen = dataLen;
        dh.RequestId = requestId;
        auto dataChunk = std::make_shared<TChunk>();
        dataChunk->Head =
            TString(reinterpret_cast<const char*>(&dh), sizeof(dh));
        dataChunk->Data = frame->Data;
        dataChunk->Total = sizeof(dh) + dataLen;
        dataChunk->Frame = frame;
        Data.Queue.push_back(std::move(dataChunk));
        StartSend(Data);
    }
    StartSend(Main);
}

void TConnection::StartSend(TStream& stream)
{
    if (Closing || stream.SendOp || stream.Queue.empty()) {
        return;
    }

    TIoOp* op = Ring.AllocOp(this, stream.SendTag);
    auto& held = InFlightSends[op];
    size_t bytes = 0;
    size_t payload = 0;

    for (const auto& chunk: stream.Queue) {
        size_t offset = chunk->Offset;
        if (offset < chunk->Head.size()) {
            const size_t len = chunk->Head.size() - offset;
            op->Iov[op->IovCount++] = iovec{
                .iov_base = const_cast<char*>(chunk->Head.data()) + offset,
                .iov_len = len};
            bytes += len;
            offset = chunk->Head.size();
        }
        size_t dataOffset = offset - chunk->Head.size();
        for (const auto& ref: chunk->Data) {
            if (dataOffset >= ref.Size()) {
                dataOffset -= ref.Size();
                continue;
            }
            if (op->IovCount == MAX_IOV) {
                break;
            }
            const size_t len = ref.Size() - dataOffset;
            op->Iov[op->IovCount++] = iovec{
                .iov_base = const_cast<char*>(ref.Data()) + dataOffset,
                .iov_len = len};
            bytes += len;
            payload += len;
            dataOffset = 0;
        }
        held.push_back(chunk);
        // the first chunk always goes in, so a send never has zero iovecs
        if (op->IovCount == MAX_IOV || bytes >= Params.SendWindowBytes) {
            break;
        }
    }

    Y_ABORT_UNLESS(op->IovCount);
    op->ZeroCopy =
        Params.SendZeroCopy && payload >= Params.SendZeroCopyThreshold;
    stream.SendOp = op;
    Ring.SendMsg(op, stream.Fd, 0);
}

void TConnection::OnSendComplete(
    TStream& stream,
    TIoOp* op,
    int res,
    ui32 cqeFlags)
{
    if (cqeFlags & IORING_CQE_F_NOTIF) {
        // the kernel released the zero-copy pages, so the chunks can go now;
        // stream.SendOp may already be a newer op
        Ring.FreeOp(op);
        InFlightSends.erase(op);
        MaybeFinishClose();
        return;
    }

    // a zero-copy send reports the transfer first and the release second
    const bool last = !(cqeFlags & IORING_CQE_F_MORE);
    stream.SendOp = nullptr;

    if (res <= 0) {
        if (last) {
            Ring.FreeOp(op);
            InFlightSends.erase(op);
        }
        if (!Closing) {
            Close(MakeSystemError(res < 0 ? -res : EPIPE, "send"));
        } else {
            MaybeFinishClose();
        }
        return;
    }

    size_t left = res;
    while (left && !stream.Queue.empty()) {
        auto& chunk = *stream.Queue.front();
        const size_t take = Min(chunk.Total - chunk.Offset, left);
        chunk.Offset += take;
        left -= take;
        if (chunk.Offset < chunk.Total) {
            break;
        }
        stream.Queue.pop_front();
    }

    if (last) {
        // releasing the chunks runs the callbacks of the frames they carried,
        // so the op has to be back in the pool first
        Ring.FreeOp(op);
        InFlightSends.erase(op);
    }
    if (Closing) {
        MaybeFinishClose();
        return;
    }
    StartSend(stream);
}

////////////////////////////////////////////////////////////////////////////////
// main recv

void TConnection::StartMainRecv()
{
    if (Closing || MainRecvOp->InFlight) {
        return;
    }
    if (Filled == RecvBuf.size()) {
        Compact(RecvBuf.size() * 2);
    }
    Ring.Recv(
        MainRecvOp,
        Main.Fd,
        RecvBuf.begin() + Filled,
        RecvBuf.size() - Filled,
        0);
}

void TConnection::OnMainRecv(int res)
{
    if (Closing) {
        MaybeFinishClose();
        return;
    }
    if (res == 0) {
        Close(MakeError(E_REJECTED, "peer closed the connection"));
        return;
    }
    if (res < 0) {
        Close(MakeSystemError(-res, "recv"));
        return;
    }
    Filled += res;
    if (!ParseFrames()) {
        return;
    }
    StartMainRecv();
}

// Moves the unparsed tail to the front and grows the buffer to `needed`.
void TConnection::Compact(size_t needed)
{
    const size_t tail = Filled - Parsed;
    if (needed > RecvBuf.size()) {
        TString bigger = TString::Uninitialized(needed);
        memcpy(bigger.begin(), RecvBuf.data() + Parsed, tail);
        RecvBuf = std::move(bigger);
    } else if (Parsed > 0 && tail > 0) {
        memmove(RecvBuf.begin(), RecvBuf.data() + Parsed, tail);
    }
    Filled = tail;
    Parsed = 0;
}

bool TConnection::ParseFrames()
{
    while (Filled - Parsed >= sizeof(TFrameHeader)) {
        TFrameHeader h;
        memcpy(&h, RecvBuf.data() + Parsed, sizeof(h));

        const bool knownType = h.Type == FRAME_REQUEST ||
                               h.Type == FRAME_RESPONSE ||
                               h.Type == FRAME_ERROR;
        const bool inl = h.Flags & FLAG_INLINE;
        const bool oob = h.Flags & FLAG_OOB;
        if (h.Magic != MAGIC || !knownType || (inl && oob) ||
            (h.Flags & ~(FLAG_INLINE | FLAG_OOB)) ||
            (h.DataLen == 0) != (h.Flags == 0) ||
            h.ProtoLen > Params.MaxProtoSize ||
            h.DataLen > Params.MaxDataSize || (oob && Data.Fd < 0))
        {
            ProtocolError(
                TStringBuilder()
                << "bad frame header: magic=" << Hex(h.Magic)
                << " type=" << int(h.Type) << " flags=" << int(h.Flags)
                << " proto=" << h.ProtoLen << " data=" << h.DataLen);
            return false;
        }

        if (inl && Params.OobEnabled && h.DataLen >= Params.OobThreshold) {
            ProtocolError(
                TStringBuilder() << "inline payload " << h.DataLen
                                 << " at or above the out-of-band threshold "
                                 << Params.OobThreshold);
            return false;
        }

        const size_t need = sizeof(h) + h.ProtoLen + (inl ? h.DataLen : 0);
        if (Filled - Parsed < need) {
            if (RecvBuf.size() - Parsed < need) {
                Compact(need);
            }
            break;
        }

        const TStringBuf proto(RecvBuf.data() + Parsed + sizeof(h), h.ProtoLen);
        if (oob) {
            auto sink = Handler->OnOobFrame(h, proto);
            if (Closing) {
                // the handler closed us from inside the callback, so nothing
                // will ever arrive for this sink
                if (sink.OnDone) {
                    sink.OnDone(CloseError);
                }
                return false;
            }
            const bool badSink =
                !sink.Target.empty() && SgListGetSize(sink.Target) != h.DataLen;
            TExpect expect;
            expect.RequestId = h.RequestId;
            expect.DataLen = h.DataLen;
            expect.Sink = std::move(sink);
            DataFifo.push_back(std::move(expect));
            if (badSink) {
                // reading into a target of another size would either overrun
                // it or desync the data stream; the sink is already queued, so
                // it gets the close error
                ProtocolError("sink does not cover the payload");
                return false;
            }
            TryProgressData();
        } else {
            Handler->OnFrame(
                h,
                proto,
                TStringBuf(proto.end(), inl ? h.DataLen : 0));
        }
        if (Closing) {
            return false;
        }
        Parsed += need;
    }

    if (Parsed == Filled) {
        Parsed = Filled = 0;
    } else if (Parsed > 0) {
        Compact(RecvBuf.size());
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////
// data recv

void TConnection::StartDataHeaderRecv()
{
    if (Closing || DataRecvOp->InFlight) {
        return;
    }
    DataState = EDataState::ReadingHeader;
    DataHdrDone = 0;
    Ring.Recv(DataRecvOp, Data.Fd, &DataHdr, sizeof(DataHdr), 0);
}

void TConnection::OnDataRecv(int res)
{
    if (Closing) {
        MaybeFinishClose();
        return;
    }
    if (res == 0) {
        Close(MakeError(E_REJECTED, "peer closed the data connection"));
        return;
    }
    if (res < 0) {
        Close(MakeSystemError(-res, "data recv"));
        return;
    }

    switch (DataState) {
        case EDataState::ReadingHeader:
            DataHdrDone += res;
            if (DataHdrDone < sizeof(DataHdr)) {
                Ring.Recv(
                    DataRecvOp,
                    Data.Fd,
                    reinterpret_cast<char*>(&DataHdr) + DataHdrDone,
                    sizeof(DataHdr) - DataHdrDone,
                    0);
                return;
            }
            if (DataHdr.Magic != MAGIC || DataHdr.DataLen == 0 ||
                DataHdr.DataLen > Params.MaxDataSize)
            {
                ProtocolError(
                    TStringBuilder()
                    << "bad data header: magic=" << Hex(DataHdr.Magic)
                    << " request " << DataHdr.RequestId << " len "
                    << DataHdr.DataLen);
                return;
            }
            DataState = EDataState::WaitingAdmission;
            TryProgressData();
            return;

        case EDataState::ReadingPayload: {
            auto& e = DataFifo.front();
            e.Done += res;
            size_t left = res;
            while (left && e.RefIndex < e.Sink.Target.size()) {
                const size_t remaining =
                    e.Sink.Target[e.RefIndex].Size() - e.RefOffset;
                if (left >= remaining) {
                    left -= remaining;
                    ++e.RefIndex;
                    e.RefOffset = 0;
                } else {
                    e.RefOffset += left;
                    left = 0;
                }
            }
            if (e.Done < e.DataLen) {
                IssuePayloadRecv();
                return;
            }
            FinishPayload();
            return;
        }

        default:
            Y_ABORT(
                "unexpected data recv completion in state %d",
                int(DataState));
    }
}

void TConnection::TryProgressData()
{
    if (DataState != EDataState::WaitingAdmission || DataFifo.empty()) {
        return;
    }
    auto& e = DataFifo.front();
    if (e.RequestId != DataHdr.RequestId || e.DataLen != DataHdr.DataLen) {
        ProtocolError(
            TStringBuilder()
            << "data stream mismatch: expected request " << e.RequestId
            << " len " << e.DataLen << ", got request " << DataHdr.RequestId
            << " len " << DataHdr.DataLen);
        return;
    }
    DataState = EDataState::ReadingPayload;
    IssuePayloadRecv();
}

void TConnection::IssuePayloadRecv()
{
    auto& e = DataFifo.front();
    const size_t remaining = e.DataLen - e.Done;
    DataRecvOp->IovCount = 0;
    if (e.Sink.Target.empty()) {
        if (Scratch.empty()) {
            Scratch = TString::Uninitialized(SCRATCH_SIZE);
        }
        DataRecvOp->Iov[0] = iovec{
            .iov_base = Scratch.begin(),
            .iov_len = Min(remaining, Scratch.size())};
        DataRecvOp->IovCount = 1;
    } else {
        size_t i = e.RefIndex;
        size_t offset = e.RefOffset;
        size_t len = 0;
        while (i < e.Sink.Target.size() && len < remaining &&
               DataRecvOp->IovCount < MAX_IOV)
        {
            const auto& ref = e.Sink.Target[i];
            const size_t take = Min(ref.Size() - offset, remaining - len);
            DataRecvOp->Iov[DataRecvOp->IovCount++] = iovec{
                .iov_base = const_cast<char*>(ref.Data()) + offset,
                .iov_len = take};
            len += take;
            offset = 0;
            ++i;
        }
    }
    Y_ABORT_UNLESS(DataRecvOp->IovCount);
    Ring.RecvMsg(DataRecvOp, Data.Fd, MSG_WAITALL);
}

void TConnection::FinishPayload()
{
    auto sink = std::move(DataFifo.front().Sink);
    DataFifo.pop_front();
    DataState = EDataState::Idle;
    if (sink.OnDone) {
        sink.OnDone({});
    }
    if (Closing) {
        MaybeFinishClose();
        return;
    }
    StartDataHeaderRecv();
}

////////////////////////////////////////////////////////////////////////////////
// completion dispatch

void TConnection::OnIoComplete(TIoOp* op, int res, ui32 cqeFlags)
{
    switch (op->Tag) {
        case TAG_MAIN_RECV:
            OnMainRecv(res);
            break;
        case TAG_DATA_RECV:
            OnDataRecv(res);
            break;
        case TAG_MAIN_SEND:
            OnSendComplete(Main, op, res, cqeFlags);
            break;
        case TAG_DATA_SEND:
            OnSendComplete(Data, op, res, cqeFlags);
            break;
        default:
            Y_ABORT("unknown op tag %u", op->Tag);
    }
}

////////////////////////////////////////////////////////////////////////////////
// close

void TConnection::ProtocolError(TStringBuf what)
{
    STORAGE_WARN("protocol error, closing: " << what);
    Close(
        MakeError(E_REJECTED, TStringBuilder() << "protocol error: " << what));
}

void TConnection::Close(NProto::TError error)
{
    Y_DEBUG_ABORT_UNLESS(Ring.IsRingThread());

    if (Closing) {
        return;
    }
    Closing = true;
    CloseError = std::move(error);

    // the queues are not dropped here: the kernel may still be reading from
    // the chunks a cancelled send points at
    DropUndeliveredData(Main.Fd);
    DropUndeliveredData(Data.Fd);
    Shutdown(Main.Fd);
    Shutdown(Data.Fd);

    Ring.Cancel(MainRecvOp);
    Ring.Cancel(DataRecvOp);
    for (auto& [op, chunks]: InFlightSends) {
        Ring.Cancel(op);
    }
    MaybeFinishClose();
}

void TConnection::MaybeFinishClose()
{
    if (!Closing || Finished) {
        return;
    }
    if (MainRecvOp->InFlight || DataRecvOp->InFlight || !InFlightSends.empty())
    {
        return;
    }
    Finished = true;

    CloseWithReset(Main.Fd);
    CloseWithReset(Data.Fd);
    Main.Fd = Data.Fd = -1;

    // nothing is in flight anymore, so dropping the undelivered frames is
    // safe; that runs their OnDone
    Main.Queue.clear();
    Data.Queue.clear();

    auto fifo = std::move(DataFifo);
    for (auto& e: fifo) {
        if (e.Sink.OnDone) {
            e.Sink.OnDone(CloseError);
        }
    }

    Ring.Execute([this] { Handler->OnConnectionClosed(CloseError); });
}

}   // namespace NCloud::NBlockStore::NUring
