#include "event_test.h"

namespace NCloud::NIcV2::NTests {

////////////////////////////////////////////////////////////////////////////////

void PreserializeEvent(std::unique_ptr<NActors::IEventHandle>& handle)
{
    auto original = std::move(handle);
    auto buffer = original->ReleaseChainBuffer();
    handle = std::make_unique<NActors::IEventHandle>(
        original->Type,
        original->Flags,
        original->Recipient,
        original->Sender,
        std::move(buffer),
        original->Cookie,
        nullptr,
        std::move(original->TraceId));
}

}   // namespace NCloud::NIcV2::NTests
