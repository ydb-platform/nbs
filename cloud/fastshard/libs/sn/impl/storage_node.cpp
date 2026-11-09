#include "storage_node.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>

#include <google/protobuf/repeated_ptr_field.h>

#include <util/generic/algorithm.h>
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/file.h>

#include <sys/uio.h>

#include <cerrno>
#include <cstring>

namespace NCloud::NFastShard {

namespace {

using silk::FiberScheduler;

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError MakeIoError(int err, TStringBuf op)
{
    return MakeError(
        MAKE_SYSTEM_ERROR(err),
        TStringBuilder() << "sn/impl " << op << ": " << ::strerror(err));
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool MulOverflowsU64(ui64 a, ui64 b)
{
    return a != 0 && b > Max<ui64>() / a;
}

constexpr bool AddOverflowsU64(ui64 a, ui64 b)
{
    return a > Max<ui64>() - b;
}

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError ValidateReadRequest(
    const NCloud::NProto::TReadPagesRequest& req)
{
    for (size_t i = 0; i < req.PageGroupRefsSize(); ++i) {
        const auto& ref = req.GetPageGroupRefs(i);
        const ui64 firstPageNo = ref.GetFirstPageNo();
        const ui64 pageCount = ref.GetPageCount();
        const ui64 pageSize = ref.GetPageSize();
        if (MulOverflowsU64(pageCount, pageSize)) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "sn/impl read: PageCount * PageSize overflows ui64"
                       " at ref "
                    << i << " (PageCount=" << pageCount
                    << ", PageSize=" << pageSize << ")");
        }
        if (MulOverflowsU64(firstPageNo, pageSize)) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "sn/impl read: FirstPageNo * PageSize overflows"
                       " ui64 at ref "
                    << i << " (FirstPageNo=" << firstPageNo
                    << ", PageSize=" << pageSize << ")");
        }
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

using TPageGroups =
    google::protobuf::RepeatedPtrField<NCloud::NProto::TDevicePageGroup>;

NCloud::NProto::TError ValidatePageGroups(const TPageGroups& groups)
{
    //
    // Two invariants:
    //   1. All content pages across the whole request share one size.
    //      The impl derives per-group offsets from that single value, so
    //      any mismatch would silently misplace bytes on disk.
    //   2. Page-index intervals [FirstPageNo, FirstPageNo+ContentSize)
    //      of distinct groups do not overlap. Overlap would mean two
    //      concurrent writes race for the same disk range with unspecified
    //      ordering.
    // Also guards against FirstPageNo*pageSize overflow.
    //

    struct TInterval
    {
        ui64 Start = 0;
        ui64 End = 0;
        size_t GroupIndex = 0;
    };

    ui64 pageSize = 0;
    TVector<TInterval> intervals;
    const size_t groupCount = groups.size();
    intervals.reserve(groupCount);

    for (size_t i = 0; i < groupCount; ++i) {
        const auto& pg = groups[i];
        const size_t pageCount = pg.ContentSize();
        if (pageCount == 0) {
            continue;
        }
        for (size_t k = 0; k < pageCount; ++k) {
            const ui64 sz = pg.GetContent(k).size();
            if (pageSize == 0) {
                pageSize = sz;
            } else if (sz != pageSize) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "sn/impl write: page size mismatch: expected "
                        << pageSize << ", got " << sz << " at group " << i
                        << " content " << k);
            }
        }
        const ui64 firstPageNo = pg.GetFirstPageNo();
        if (MulOverflowsU64(firstPageNo, pageSize)) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "sn/impl write: FirstPageNo * pageSize overflows"
                       " ui64 at group "
                    << i << " (FirstPageNo=" << firstPageNo
                    << ", pageSize=" << pageSize << ")");
        }
        if (AddOverflowsU64(firstPageNo, static_cast<ui64>(pageCount))) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "sn/impl write: FirstPageNo + pageCount overflows"
                       " ui64 at group "
                    << i << " (FirstPageNo=" << firstPageNo
                    << ", pageCount=" << pageCount << ")");
        }
        const ui64 endPageNo = firstPageNo + pageCount;
        intervals.push_back({firstPageNo, endPageNo, i});
    }

    if (intervals.size() > 1) {
        Sort(
            intervals,
            [](const TInterval& a, const TInterval& b)
            { return a.Start < b.Start; });
        for (size_t i = 1; i < intervals.size(); ++i) {
            if (intervals[i].Start < intervals[i - 1].End) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "sn/impl write: overlapping page intervals:"
                           " group "
                        << intervals[i - 1].GroupIndex << " ["
                        << intervals[i - 1].Start << ", "
                        << intervals[i - 1].End << ") vs group "
                        << intervals[i].GroupIndex << " [" << intervals[i].Start
                        << ", " << intervals[i].End << ")");
            }
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TNaiveFileStorageNode: public IStorageNode
{
public:
    explicit TNaiveFileStorageNode(TString path)
        : Path(std::move(path))
        , File(Path, OpenExisting | RdWr)
    {
        Y_ENSURE(
            File.IsOpen(),
            "sn/impl: failed to open " << Path.Quote() << ": "
                                       << ::strerror(errno));
    }

    NCloud::NProto::TAcquireDevicesResponse AcquireDevices(
        TFastShardContext&,
        NCloud::NProto::TAcquireDevicesRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TReleaseDevicesResponse ReleaseDevices(
        TFastShardContext&,
        NCloud::NProto::TReleaseDevicesRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TFormatDeviceResponse FormatDevice(
        TFastShardContext&,
        NCloud::NProto::TFormatDeviceRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TListDevicesResponse ListDevices(
        TFastShardContext&,
        NCloud::NProto::TListDevicesRequest request) override
    {
        // The file has no device UUID, so there is nothing to list.
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TReadPagesResponse ReadPages(
        TFastShardContext&,
        NCloud::NProto::TReadPagesRequest request) override
    {
        NCloud::NProto::TReadPagesResponse resp;

        if (auto err = ValidateReadRequest(request); HasError(err)) {
            *resp.MutableError() = std::move(err);
            return resp;
        }

        //
        // One op per PageGroupRef. Each op reads PageCount*PageSize bytes
        // into its own contiguous buffer via a single io_uring read. Ops
        // must not be reallocated after submission because the IoFuture
        // address is registered with io_uring.
        //

        struct TReadOp
        {
            ui64 FirstPageNo = 0;
            ui64 PageCount = 0;
            ui32 PageSize = 0;
            TBuffer Buffer;
            iovec Iov{};
            ui64 BytesRead = 0;
            FiberScheduler::IoFuture Future;
        };

        const size_t n = request.PageGroupRefsSize();
        if (n == 0) {
            return resp;
        }

        TVector<TReadOp> ops(n);

        const int fd = static_cast<FHANDLE>(File);

        for (size_t i = 0; i < n; ++i) {
            const auto& ref = request.GetPageGroupRefs(i);
            auto& op = ops[i];
            op.FirstPageNo = ref.GetFirstPageNo();
            op.PageCount = ref.GetPageCount();
            op.PageSize = ref.GetPageSize();
            op.Buffer.Resize(op.PageCount * op.PageSize);
            op.Iov.iov_base = op.Buffer.Data();
            op.Iov.iov_len = op.Buffer.Size();
            const ui64 offset = op.FirstPageNo * op.PageSize;
            FiberScheduler::read(
                fd,
                &op.Iov,
                1 /* iov_len */,
                offset,
                &op.BytesRead,
                &op.Future);
        }

        //
        // Wait for every op. We cannot bail out early on error: io_uring
        // still owns pointers into op.Iov / op.Buffer / op.Future for
        // any pending SQEs, so destroying ops mid-flight would corrupt
        // freed memory when the kernel completes them. On first error,
        // cancel the tail to shorten the wait, then keep waiting.
        //

        int firstErr = 0;
        for (size_t i = 0; i < ops.size(); ++i) {
            int err = ops[i].Future.wait();
            if (err != 0 && firstErr == 0) {
                firstErr = err;
                for (size_t j = i + 1; j < ops.size(); ++j) {
                    ops[j].Future.cancel();
                }
            }
        }

        if (firstErr != 0) {
            *resp.MutableError() = MakeIoError(firstErr, "read");
            return resp;
        }

        //
        // io_uring reports 0 for a nonnegative pread even if the kernel
        // returned fewer bytes than requested (short read past EOF, torn
        // read, etc). Silk exposes the actual count via bytesRead, so we
        // must compare it against the requested length before treating
        // the buffer as populated.
        //

        for (auto& op: ops) {
            if (op.BytesRead != op.Iov.iov_len) {
                *resp.MutableError() = MakeError(
                    E_IO,
                    TStringBuilder() << "sn/impl read: short read at offset "
                                     << op.FirstPageNo * op.PageSize << ": got "
                                     << op.BytesRead << " of " << op.Iov.iov_len
                                     << " bytes");
                return resp;
            }
        }

        for (auto& op: ops) {
            auto* pg = resp.AddPageGroups();
            pg->SetFirstPageNo(op.FirstPageNo);
            for (ui64 i = 0; i < op.PageCount; ++i) {
                pg->AddContent()->assign(
                    op.Buffer.Data() + static_cast<size_t>(i) * op.PageSize,
                    op.PageSize);
            }
        }
        return resp;
    }

    NCloud::NProto::TWriteLogRecordResponse WriteLogRecord(
        TFastShardContext&,
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        NCloud::NProto::TWriteLogRecordResponse resp;
        if (auto err = WritePageGroups(*request.MutablePageGroups());
            HasError(err))
        {
            *resp.MutableError() = std::move(err);
        }
        return resp;
    }

    NCloud::NProto::TReadJournalTailResponse ReadJournalTail(
        TFastShardContext&,
        NCloud::NProto::TReadJournalTailRequest request) override
    {
        // WriteLogRecord writes in place, so the journal is always empty and
        // there is never anything to replay.
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermark(
        TFastShardContext&,
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request) override
    {
        // Every page is already at its final location, so the watermark can be
        // advanced to any value without doing anything.
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TWritePagesResponse WritePages(
        TFastShardContext&,
        NCloud::NProto::TWritePagesRequest request) override
    {
        NCloud::NProto::TWritePagesResponse resp;
        if (auto err = WritePageGroups(*request.MutablePageGroups());
            HasError(err))
        {
            *resp.MutableError() = std::move(err);
        }
        return resp;
    }

private:
    NCloud::NProto::TError WritePageGroups(TPageGroups& groups)
    {
        if (auto err = ValidatePageGroups(groups); HasError(err)) {
            return err;
        }

        //
        // One gather write per PageGroup. The Content pages of a group
        // occupy contiguous file offsets starting at FirstPageNo*pageSize,
        // where pageSize is the size of Content[0], so all pages of a
        // group ride a single io_uring writev.
        //

        struct TWriteOp
        {
            TVector<iovec> Iov;
            ui64 TotalLen = 0;
            ui64 BytesWritten = 0;
            FiberScheduler::IoFuture Future;
        };

        const size_t n = groups.size();
        if (n == 0) {
            return {};
        }

        TVector<TWriteOp> ops(n);

        // Every buffer first: nothing may throw once an SQE points into ops.
        for (size_t i = 0; i < n; ++i) {
            auto& pg = groups[i];
            auto& op = ops[i];
            op.Iov.resize(pg.ContentSize());
            for (size_t k = 0; k < op.Iov.size(); ++k) {
                auto& content = *pg.MutableContent(k);
                op.Iov[k].iov_base = content.begin();
                op.Iov[k].iov_len = content.size();
                op.TotalLen += content.size();
            }
        }

        const int fd = static_cast<FHANDLE>(File);

        size_t submitted = 0;
        for (size_t i = 0; i < n; ++i) {
            auto& op = ops[i];
            if (op.Iov.empty()) {
                continue;
            }
            const ui64 offset = groups[i].GetFirstPageNo() * op.Iov[0].iov_len;
            FiberScheduler::write(
                fd,
                op.Iov.data(),
                op.Iov.size(),
                offset,
                &op.BytesWritten,
                &op.Future);
            ++submitted;
        }

        if (submitted == 0) {
            return {};
        }

        //
        // Same rule as ReadPages: never bail out with pending SQEs still
        // pointing into ops. Wait for every submitted op; cancel the
        // tail on first error.
        //

        int firstErr = 0;
        for (size_t i = 0; i < n; ++i) {
            if (ops[i].Iov.empty()) {
                continue;
            }
            int err = ops[i].Future.wait();
            if (err != 0 && firstErr == 0) {
                firstErr = err;
                for (size_t j = i + 1; j < n; ++j) {
                    if (!ops[j].Iov.empty()) {
                        ops[j].Future.cancel();
                    }
                }
            }
        }

        if (firstErr != 0) {
            return MakeIoError(firstErr, "write");
        }

        //
        // Symmetric with ReadPages: writev may complete with a positive
        // short count under quota / fsize-limit conditions and silk
        // reports success. If BytesWritten falls below TotalLen only a
        // prefix reached the file, so any later read of this group would
        // see a mix of new and stale bytes -- refuse to acknowledge.
        //

        for (size_t i = 0; i < n; ++i) {
            const auto& op = ops[i];
            if (op.Iov.empty()) {
                continue;
            }
            if (op.BytesWritten != op.TotalLen) {
                return MakeError(
                    E_IO,
                    TStringBuilder()
                        << "sn/impl write: short write: " << op.BytesWritten
                        << " of " << op.TotalLen << " bytes");
            }
        }

        return {};
    }

    const TString Path;
    TFileHandle File;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateNaiveFileStorageNode(TString path)
{
    return std::make_shared<TNaiveFileStorageNode>(std::move(path));
}

}   // namespace NCloud::NFastShard
