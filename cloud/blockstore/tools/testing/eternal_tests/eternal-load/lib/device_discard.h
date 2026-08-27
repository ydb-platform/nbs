#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/system/file.h>
#include <util/system/types.h>

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

struct IDiscardService
{
    virtual ~IDiscardService() = default;

    virtual void AsyncZero(
        TFileHandle& file,
        i64 offset,
        ui32 count,
        TFileIOCompletion* completion) = 0;

    template <typename F>
        requires std::is_invocable_v<F, NProto::TError, ui32>
    void AsyncZero(TFileHandle& file, i64 offset, ui32 count, F&& callback)
    {
        auto cb = std::make_unique<TCallbackCompletion<std::decay_t<F>>>(
            std::forward<F>(callback));

        AsyncZero(file, offset, count, cb.get());

        Y_UNUSED(cb.release());   // ownership transferred
    }

private:
    // TODO: deduplicate with TFileIOCompletion in file_io_service.h:
    // https://github.com/ydb-platform/nbs/blob/main/cloud/storage/core/libs/common/file_io_service.h
    template <typename F>
    struct TCallbackCompletion: TFileIOCompletion
    {
        F Func;

        template <typename T>
        explicit TCallbackCompletion(T&& func)
            : TFileIOCompletion{.Func = &TCallbackCompletion::Complete}
            , Func{std::forward<T>(func)}
        {}

        static void Complete(
            TFileIOCompletion* self,
            const NProto::TError& error,
            ui32 bytes)
        {
            std::unique_ptr<TCallbackCompletion> ptr{
                static_cast<TCallbackCompletion*>(self)};

            std::invoke(std::move(ptr->Func), error, bytes);
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

// Issues BLKDISCARD for [offset, offset + length) on a block device.
// Returns an error if the file handle is not a block device or discard fails.
NProto::TError DiscardDeviceRange(TFileHandle& file, ui64 offset, ui64 length);

}   // namespace NCloud::NBlockStore::NTesting
