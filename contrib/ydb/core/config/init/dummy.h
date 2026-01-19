#pragma once
#include <util/generic/ptr.h>
#include <contrib/ydb/core/protos/config.pb.h>

TAutoPtr<NKikimrConfig::TActorSystemConfig> DummyActorSystemConfig();
TAutoPtr<NKikimrConfig::TChannelProfileConfig> DummyChannelProfileConfig();
TAutoPtr<NKikimrConfig::TAllocatorConfig> DummyAllocatorConfig();
