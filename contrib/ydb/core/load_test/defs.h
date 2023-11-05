#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/load_test/defs.h
#include <contrib/ydb/core/base/defs.h>
#include <contrib/ydb/core/base/logoblob.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <contrib/ydb/core/protos/load_test.pb.h>
#include <contrib/ydb/library/services/services.pb.h>

#include <contrib/ydb/core/load_test/events.h>
