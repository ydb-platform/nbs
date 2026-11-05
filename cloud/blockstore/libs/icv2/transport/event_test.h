#pragma once

#include <contrib/ydb/library/actors/core/event.h>

#include <memory>

namespace NCloud::NBlockStore::NIcV2::NTests {

template <typename TPtr>
void PreserializeEvent(TPtr& handle)
{
    auto original = std::move(handle);
    auto buffer = original->ReleaseChainBuffer();
    handle.reset(new NActors::IEventHandle(
        original->Type,
        original->Flags,
        original->Recipient,
        original->Sender,
        std::move(buffer),
        original->Cookie,
        nullptr,
        std::move(original->TraceId)));
}

}   // namespace NCloud::NBlockStore::NIcV2::NTests
