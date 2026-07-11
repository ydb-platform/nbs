#include "storage_node.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/file.h>

#include <cerrno>
#include <cstring>
#include <sys/uio.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

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

class TNaiveFileStorageNode: public IStorageNode
{
public:
    explicit TNaiveFileStorageNode(TString path)
        : Path(std::move(path))
        , File(Path, OpenExisting | RdWr)
    {
        Y_ENSURE(
            File.IsOpen(),
            "sn/impl: failed to open " << Path.Quote()
                << ": " << ::strerror(errno));
    }

    NCloud::NProto::TAcquireDevicesResponse AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TReleaseDevicesResponse ReleaseDevices(
        NCloud::NProto::TReleaseDevicesRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    NCloud::NProto::TReadPagesResponse ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        NCloud::NProto::TReadPagesResponse resp;

        //
        // One op per PageGroupRef. Each op reads PageCount*PageSize bytes
        // into its own contiguous buffer via a single io_uring read. Ops
        // must not be reallocated after submission because the IoFuture
        // address is registered with io_uring.
        //

        struct TReadOp
        {
            ui64 FirstPageNo = 0;
            ui32 PageCount = 0;
            ui32 PageSize = 0;
            TString Buffer;
            iovec Iov{};
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
            op.Buffer.ReserveAndResize(
                static_cast<size_t>(op.PageCount) * op.PageSize);
            op.Iov.iov_base = op.Buffer.begin();
            op.Iov.iov_len = op.Buffer.size();
            const ui64 offset = op.FirstPageNo * op.PageSize;
            FiberScheduler::read(
                fd,
                &op.Iov,
                1 /* iov_len */,
                offset,
                nullptr /* bytesRead */,
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

        for (auto& op: ops) {
            auto* pg = resp.AddPageGroups();
            pg->SetFirstPageNo(op.FirstPageNo);
            for (ui32 i = 0; i < op.PageCount; ++i) {
                pg->AddContent(
                    op.Buffer.substr(
                        static_cast<size_t>(i) * op.PageSize,
                        op.PageSize));
            }
        }
        return resp;
    }

    NCloud::NProto::TWriteLogRecordResponse WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        NCloud::NProto::TWriteLogRecordResponse resp;

        //
        // One gather write per PageGroup. The Content pages of a group
        // occupy contiguous file offsets starting at FirstPageNo*pageSize,
        // where pageSize is the size of Content[0], so all pages of a
        // group ride a single io_uring writev.
        //

        struct TWriteOp
        {
            TVector<iovec> Iov;
            FiberScheduler::IoFuture Future;
        };

        const size_t n = request.PageGroupsSize();
        if (n == 0) {
            return resp;
        }

        TVector<TWriteOp> ops(n);

        const int fd = static_cast<FHANDLE>(File);

        size_t submitted = 0;
        for (size_t i = 0; i < n; ++i) {
            auto& pg = *request.MutablePageGroups(i);
            const size_t pageCount = pg.ContentSize();
            if (pageCount == 0) {
                continue;
            }
            auto& op = ops[i];
            const size_t pageSize = pg.GetContent(0).size();
            op.Iov.resize(pageCount);
            for (size_t k = 0; k < pageCount; ++k) {
                auto& content = *pg.MutableContent(k);
                op.Iov[k].iov_base = content.begin();
                op.Iov[k].iov_len = content.size();
            }
            const ui64 offset = pg.GetFirstPageNo() * pageSize;
            FiberScheduler::write(
                fd,
                op.Iov.data(),
                static_cast<uint32_t>(op.Iov.size()),
                offset,
                nullptr /* bytesWritten */,
                &op.Future);
            ++submitted;
        }

        if (submitted == 0) {
            return resp;
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
            *resp.MutableError() = MakeIoError(firstErr, "write");
        }
        return resp;
    }

private:
    const TString Path;
    TFileHandle File;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateNaiveFileStorageNode(TString path)
{
    return std::make_shared<TNaiveFileStorageNode>(std::move(path));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
