#pragma once

#include <contrib/ydb/library/actors/core/event.h>

#include <memory>

namespace NCloud::NIcV2::NTests {

////////////////////////////////////////////////////////////////////////////////

// Serialize before passing the event to code that handles received events.
void PreserializeEvent(std::unique_ptr<NActors::IEventHandle>& handle);

}   // namespace NCloud::NIcV2::NTests
