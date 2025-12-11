#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NNodeTabletMonitor {
struct ITabletStateClassifier;
using ITabletStateClassifierPtr = TIntrusivePtr<ITabletStateClassifier>;

struct ITabletListRenderer;
using ITabletListRendererPtr = TIntrusivePtr<ITabletListRenderer>;
}   // namespace NNodeTabletMonitor

class TControlBoard;
using TControlBoardPtr = TIntrusivePtr<TControlBoard>;

class TKikimrScopeId;
}   // namespace NKikimr
