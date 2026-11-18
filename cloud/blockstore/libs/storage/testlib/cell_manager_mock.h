#pragma once

#include <cloud/blockstore/libs/cells/iface/public.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

// Replaces the transport only: requests execute in the target service.
NCells::ICellManagerPtr CreateCellManagerMock(
    NActors::TTestActorRuntimeBase& runtime,
    THashMap<TString, ui32> cellNodes);

}   // namespace NCloud::NBlockStore::NStorage
