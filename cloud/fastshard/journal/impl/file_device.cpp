#include "file_device.h"

#include <cloud/fastshard/journal/iface/device.h>

#include <cloud/storage/core/libs/common/aligned_buffer.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/generic/array_ref.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/sanitizers.h>

#include <cstring>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// O_DIRECT requires the buffers, the offsets and the sizes to be aligned to
// the logical block size of the underlying device, which never exceeds 4 KiB
constexpr ui32 DirectIOAlignment = 4_KB;

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError CheckTransfer(
    TStringBuf operation,
    ui64 firstPageNo,
    ui64 expectedBytes,
    const NCloud::NProto::TError& error,
    ui32 bytesTransferred)
{
    if (HasError(error)) {
        return error;
    }

    if (bytesTransferred != expectedBytes) {
        return MakeError(E_IO, TStringBuilder()
            << "short " << operation << " at page " << firstPageNo
            << ": expected " << expectedBytes << " bytes, got "
            << bytesTransferred);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TFileDevice final: public IDevice
{
private:
    const IFileIOServicePtr FileIO;
    const ui64 PageCount;
    const ui32 PageSize;

    TFileHandle File;

public:
    TFileDevice(
            IFileIOServicePtr fileIO,
            const TString& filePath,
            ui64 pageCount,
            ui32 pageSize)
        : FileIO(std::move(fileIO))
        , PageCount(pageCount)
        , PageSize(pageSize)
        , File(filePath, OpenAlways | RdWr | DirectAligned)
    {
        Y_ENSURE(
            PageSize > 0 && PageSize % DirectIOAlignment == 0,
            "page size must be a non-zero multiple of " << DirectIOAlignment
                << ", got " << PageSize);

        Y_ENSURE(
            File.IsOpen(),
            "unable to open " << filePath.Quote() << ": "
                << LastSystemErrorText());

        const i64 length = static_cast<i64>(PageCount * PageSize);
        if (File.GetLength() < length) {
            Y_ENSURE(
                File.Resize(length),
                "unable to resize " << filePath.Quote() << " to " << length
                    << " bytes: " << LastSystemErrorText());
        }
    }

    // IDevice

    TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        using TResult = TResultOrError<TVector<TBuffer>>;

        ui64 pageCount = 0;
        for (const auto& ref: rangeRefs) {
            auto error = ValidatePageRange(ref.FirstPageNo, ref.PageCount);
            if (HasError(error)) {
                return MakeFuture<TResult>(std::move(error));
            }

            pageCount += ref.PageCount;
        }

        // the pages the data is copied into must outlive the requests
        auto pages = std::make_shared<TVector<TBuffer>>(pageCount);

        TVector<TFuture<NCloud::NProto::TError>> futures;
        futures.reserve(rangeRefs.size());

        ui64 pageIndex = 0;
        for (const auto& ref: rangeRefs) {
            if (ref.PageCount == 0) {
                continue;
            }

            TVector<TArrayRef<char>> dst;
            dst.reserve(ref.PageCount);

            for (ui64 i = 0; i != ref.PageCount; ++i) {
                TBuffer& page = (*pages)[pageIndex++];
                page.Resize(PageSize);
                dst.emplace_back(page.Data(), PageSize);
            }

            futures.push_back(Read(ref.FirstPageNo, std::move(dst)));
        }

        return WaitAll(futures).Apply(
            [futures = std::move(futures), pages = std::move(pages)]
            (const TFuture<void>&) mutable -> TResult
            {
                for (const auto& future: futures) {
                    const auto& error = future.GetValue();
                    if (HasError(error)) {
                        return error;
                    }
                }

                return std::move(*pages);
            });
    }

    TFuture<NCloud::NProto::TError> WritePages(
        TVector<TPageRange> ranges) override
    {
        for (const auto& range: ranges) {
            auto error = ValidatePages(range.FirstPageNo, range.Pages);
            if (HasError(error)) {
                return MakeFuture(std::move(error));
            }
        }

        TVector<TFuture<NCloud::NProto::TError>> futures;
        futures.reserve(ranges.size());

        for (const auto& range: ranges) {
            if (range.Pages.empty()) {
                continue;
            }

            futures.push_back(Write(range.FirstPageNo, range.Pages));
        }

        return WaitAll(futures).Apply(
            [futures = std::move(futures)]
            (const TFuture<void>&) -> NCloud::NProto::TError
            {
                for (const auto& future: futures) {
                    const auto& error = future.GetValue();
                    if (HasError(error)) {
                        return error;
                    }
                }

                return {};
            });
    }

private:
    NCloud::NProto::TError ValidatePages(
        ui64 firstPageNo,
        const TVector<TBuffer>& pages) const
    {
        for (const TBuffer& page: pages) {
            if (page.Size() != PageSize) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "page size mismatch: expected " << PageSize
                    << ", got " << page.Size());
            }
        }

        return ValidatePageRange(firstPageNo, pages.size());
    }

    NCloud::NProto::TError ValidatePageRange(
        ui64 firstPageNo,
        ui64 pageCount) const
    {
        if (firstPageNo > PageCount || pageCount > PageCount - firstPageNo) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "page range [" << firstPageNo << ", "
                << firstPageNo + pageCount << ") is beyond the device of "
                << PageCount << " pages");
        }

        return {};
    }

    i64 GetOffset(ui64 pageNo) const
    {
        return static_cast<i64>(pageNo * PageSize);
    }

    // the pages are read into an aligned buffer, as O_DIRECT requires, and
    // copied into the destination ones once the read succeeds
    TFuture<NCloud::NProto::TError> Read(
        ui64 firstPageNo,
        TVector<TArrayRef<char>> pages)
    {
        TAlignedBuffer buffer(
            static_cast<ui32>(pages.size() * PageSize),
            DirectIOAlignment);

        // moving the buffer into the callback keeps its data in place
        const TArrayRef<char> data(buffer.Begin(), buffer.Size());

        auto promise = NewPromise<NCloud::NProto::TError>();

        FileIO->AsyncRead(
            File,
            GetOffset(firstPageNo),
            data,
            [promise,
             firstPageNo,
             pageSize = PageSize,
             pages = std::move(pages),
             buffer = std::move(buffer)]
            (const NCloud::NProto::TError& error, ui32 bytes) mutable
            {
                auto result = CheckTransfer(
                    "read",
                    firstPageNo,
                    buffer.Size(),
                    error,
                    bytes);

                if (!HasError(result)) {
                    NSan::Unpoison(buffer.Begin(), buffer.Size());

                    const char* src = buffer.Begin();
                    for (auto& page: pages) {
                        std::memcpy(page.data(), src, pageSize);
                        src += pageSize;
                    }
                }

                promise.SetValue(std::move(result));
            });

        return promise.GetFuture();
    }

    // the pages are copied into an aligned buffer, as O_DIRECT requires, so
    // the request does not have to outlive the write
    TFuture<NCloud::NProto::TError> Write(
        ui64 firstPageNo,
        const TVector<TBuffer>& pages)
    {
        TAlignedBuffer buffer(
            static_cast<ui32>(pages.size() * PageSize),
            DirectIOAlignment);

        char* dst = buffer.Begin();
        for (const TBuffer& page: pages) {
            std::memcpy(dst, page.Data(), PageSize);
            dst += PageSize;
        }

        // moving the buffer into the callback keeps its data in place
        const TArrayRef<const char> data(buffer.Begin(), buffer.Size());

        auto promise = NewPromise<NCloud::NProto::TError>();

        FileIO->AsyncWrite(
            File,
            GetOffset(firstPageNo),
            data,
            [promise, firstPageNo, buffer = std::move(buffer)]
            (const NCloud::NProto::TError& error, ui32 bytes) mutable
            {
                promise.SetValue(CheckTransfer(
                    "write",
                    firstPageNo,
                    buffer.Size(),
                    error,
                    bytes));
            });

        return promise.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateFileDevice(
    IFileIOServicePtr fileIO,
    const TString& filePath,
    ui64 pageCount,
    ui32 pageSize)
{
    return std::make_shared<TFileDevice>(
        std::move(fileIO),
        filePath,
        pageCount,
        pageSize);
}

}   // namespace NCloud::NJournalled
