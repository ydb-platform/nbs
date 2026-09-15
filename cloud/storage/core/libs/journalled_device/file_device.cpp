#include "file_device.h"

#include "device.h"

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

    TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        for (const auto& ref: request.GetPageGroupRefs()) {
            auto error = ValidatePageGroupRef(
                ref.GetFirstPageNo(),
                ref.GetPageCount(),
                ref.GetPageSize());

            if (HasError(error)) {
                return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                    TErrorResponse(std::move(error)));
            }
        }

        // the response owns the pages the data is copied into, so it must
        // outlive the requests
        auto response =
            std::make_shared<NCloud::NProto::TReadPagesResponse>();

        auto& groups = *response->MutablePageGroups();
        groups.Reserve(request.PageGroupRefsSize());

        TVector<TFuture<NCloud::NProto::TError>> futures;
        futures.reserve(request.PageGroupRefsSize());

        for (const auto& ref: request.GetPageGroupRefs()) {
            auto& group = *groups.Add();
            group.SetFirstPageNo(ref.GetFirstPageNo());

            if (ref.GetPageCount() == 0) {
                continue;
            }

            TVector<TArrayRef<char>> pages;
            pages.reserve(ref.GetPageCount());

            for (ui64 i = 0; i != ref.GetPageCount(); ++i) {
                TString& page = *group.AddContent();
                page.ReserveAndResize(PageSize);
                pages.emplace_back(page.Detach(), PageSize);
            }

            futures.push_back(Read(ref.GetFirstPageNo(), std::move(pages)));
        }

        return WaitAll(futures).Apply(
            [futures = std::move(futures), response = std::move(response)]
            (const TFuture<void>&) mutable
                -> NCloud::NProto::TReadPagesResponse
            {
                for (const auto& future: futures) {
                    const auto& error = future.GetValue();
                    if (HasError(error)) {
                        return TErrorResponse(error);
                    }
                }

                return std::move(*response);
            });
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> WritePages(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        for (const auto& group: request.GetPageGroups()) {
            auto error = ValidatePageGroup(
                group.GetFirstPageNo(),
                group.GetContent());

            if (HasError(error)) {
                return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                    TErrorResponse(std::move(error)));
            }
        }

        TVector<TFuture<NCloud::NProto::TError>> futures;
        futures.reserve(request.PageGroupsSize());

        for (const auto& group: request.GetPageGroups()) {
            if (group.ContentSize() == 0) {
                continue;
            }

            futures.push_back(
                Write(group.GetFirstPageNo(), group.GetContent()));
        }

        return WaitAll(futures).Apply(
            [futures = std::move(futures)]
            (const TFuture<void>&)
                -> NCloud::NProto::TWriteLogRecordResponse
            {
                for (const auto& future: futures) {
                    const auto& error = future.GetValue();
                    if (HasError(error)) {
                        return TErrorResponse(error);
                    }
                }

                return {};
            });
    }

private:
    NCloud::NProto::TError ValidatePageGroupRef(
        ui64 firstPageNo,
        ui64 pageCount,
        ui32 pageSize) const
    {
        if (pageSize != PageSize) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "page size mismatch: expected " << PageSize
                << ", got " << pageSize);
        }

        return ValidatePageRange(firstPageNo, pageCount);
    }

    NCloud::NProto::TError ValidatePageGroup(
        ui64 firstPageNo,
        const google::protobuf::RepeatedPtrField<TString>& content) const
    {
        for (const TString& page: content) {
            if (page.size() != PageSize) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "page size mismatch: expected " << PageSize
                    << ", got " << page.size());
            }
        }

        return ValidatePageRange(firstPageNo, content.size());
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
        const google::protobuf::RepeatedPtrField<TString>& pages)
    {
        TAlignedBuffer buffer(
            static_cast<ui32>(pages.size() * PageSize),
            DirectIOAlignment);

        char* dst = buffer.Begin();
        for (const TString& page: pages) {
            std::memcpy(dst, page.data(), PageSize);
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
