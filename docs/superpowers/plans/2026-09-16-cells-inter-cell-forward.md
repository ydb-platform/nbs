# Cells inter-cell control forward — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `Mount`/`Unmount`/`Describe` forwarded from another cell over the trusted secure control channel run without re-authorizing at the receiving host, and surface the cells subsystem on its monitoring page (config, disk search, inbound connections).

**Architecture:** A new `IBlockStore` decorator `TCellForwardService` sits in the receiving node's service stack just outside `TAuthService`. It holds two references to the same inner stack — one through `AuthService` (`authorized`), one below it (`trusted`) — and routes a request to `trusted` only when a conjunction holds (trusted server-stamped source AND `CellId` header AND whitelisted method), otherwise to `authorized`. The cell connection stamps `CellId` on its outbound `Mount`/`Unmount` so the gate can recognize the forward. The decorator records per-source activity and serves it on its own mon page; the existing `TCellsMonPage` gains a config dump and a disk-search box.

**Tech Stack:** C++, Arcadia build (`ya make`), NBS `IBlockStore` service decorators, `TBlockStoreImpl` CRTP dispatch, `library/cpp/monlib` HTML mon pages, `NThreading::TFuture`.

**Spec:** `docs/superpowers/specs/2026-09-14-cells-inter-cell-control-forward-design.md`

## Global Constraints

- No default arguments in production code (test helpers may keep them).
- Comments in English. `ColumnLimit` 80. Build is `-Werror`.
- New unit tests go at the end of the test file, never inserted mid-suite.
- Build/test: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 <path>` (small suites; `-tt` for medium). Filter a case with `-F "TSuite::Test"`.
- Before editing, confirm the branch with `git branch --show-current` (feature branch `cells-inter-cell-forward`).
- Trust anchor is server-stamped `ERequestSource`, never a client header alone. The gate is a conjunction of three conditions; every branch gets a test (see Risks in the spec).
- The whole feature is gated on `GetCellsEnabled()`: with cells off, the daemon stack must be byte-for-byte unchanged.

**Design note (reaffirmed):** the forward service routes trusted requests to a
`trusted` reference into the pre-`AuthService` stack — the same pre-auth
service-reference bypass NBS already uses in `SessionManager` (its internal
mount from KickEndpoint calls the `Service` captured at `bootstrap.cpp:465`,
before `InitAuthService` at `:740`) and in `udsService`. Auth-skip is always
structural (which object you call), never a `RequestSource` value; do not
overwrite the source. No change to the shared auth layer.

**PR split — cells changes review faster than the rest, so split by folder:**

- **PR A (cells, `libs/cells` only):** Tasks 1, 2, 3, 6, 7 and the doc comment
  from Task 5. The forward service is complete and unit-tested but **dormant**
  — its factory is not called until PR B wires it, so PR A is safe to merge on
  its own. (The PR description should say the wiring lands in the follow-up.)
- **PR B (non-cells, `libs/daemon/common` only):** Task 4 and the daemon-side
  verification from Task 5 — capture `trusted` before `InitAuthService` and
  wrap the stack with `CreateCellForwardService`. One file; depends on PR A.

Land PR A first (it introduces `CreateCellForwardService`), then PR B.

**Status (2026-09-16):** PR A shipped as Tasks 1, 2, 3, 6 (CellId stamping,
forward gate, inbound activity page + audit log, config dump) — merged/ready
on branch `cells-inter-cell-forward`. Task 7 (disk search) is **deferred**: it
needs a local fallback `service` + `clientConfig` the mon page does not hold,
and a bounded blocking wait on a cross-cell network describe inside the render
— heavier than the rest, to be done as a focused follow-up. Task 4 (daemon
wiring, PR B) follows after PR A lands in main, like the rdma cleanup followed
the cells liveness PR.

---

### Task 1: Stamp CellId on the connection's Mount/Unmount

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/host_pool.h` (add `GetCellId`)
- Modify: `cloud/blockstore/libs/cells/impl/host_pool.cpp` (define `GetCellId`)
- Modify: `cloud/blockstore/libs/cells/impl/connection.cpp` (`TControlService`, `GetService`)
- Test: `cloud/blockstore/libs/cells/impl/connection_ut.cpp`

**Interfaces:**
- Produces: `TString TCellHostPool::GetCellId() const` — the cell id from the pool's config.
- Produces: `TControlService` stamps `headers.CellId` on `MountVolume`/`UnmountVolume` requests it forwards.

- [ ] **Step 1: Write the failing test** — append to `connection_ut.cpp`:

```cpp
    Y_UNIT_TEST(ShouldStampCellIdOnMountAndUnmount)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC);

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT_VALUES_EQUAL(
            "cell-1",
            env.GrpcClient->Service->LastRequestCellId);

        env.GrpcClient->Service->LastRequestCellId.clear();
        service->UnmountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TUnmountVolumeRequest>());
        UNIT_ASSERT_VALUES_EQUAL(
            "cell-1",
            env.GrpcClient->Service->LastRequestCellId);
    }
```

Before this compiles, add a `TString LastRequestCellId;` field to the test double `TTestBlockStore`/service that records `request->GetHeaders().GetCellId()` in its `MountVolume`/`UnmountVolume` handlers (mirror how `TabletHostToReport` is wired). The env's cell id is `"cell-1"` (set in `TTestEnv`'s `proto.SetCellId("cell-1")`).

- [ ] **Step 2: Run it and confirm it fails**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells -F "TCellConnectionTest::ShouldStampCellIdOnMountAndUnmount"`
Expected: FAIL — `LastRequestCellId` is empty (`"" != "cell-1"`).

- [ ] **Step 3: Add the pool accessor.** In `host_pool.h`, next to `GetWatcherCount`:

```cpp
    [[nodiscard]] TString GetCellId() const;
```

In `host_pool.cpp`:

```cpp
TString TCellHostPool::GetCellId() const
{
    // Config is immutable, so no lock is needed here.
    return Config->GetCellId();
}
```

- [ ] **Step 4: Stamp in `TControlService`.** Give it the cell id and stamp on the two methods. Change its ctor and `Execute`:

```cpp
    TControlService(
            IBlockStorePtr impl,
            TCellConnectionPtr connection,
            TString cellId)
        : Impl(std::move(impl))
        , Connection(std::move(connection))
        , CellId(std::move(cellId))
    {}
```

Add `const TString CellId;` to its members. In `Execute`, before forwarding:

```cpp
        if constexpr (
            std::is_same_v<TMethod, TBlockStoreMountVolumeMethod> ||
            std::is_same_v<TMethod, TBlockStoreUnmountVolumeMethod>)
        {
            // marks the request as an inter-cell forward, so the receiving
            // host's forward service can let it past authorization - see the
            // inter-cell-forward design. Describe carries its own cell id
            // through the describe path already
            request->MutableHeaders()->SetCellId(CellId);
        }

        return TMethod::Execute(
            Impl.get(),
            std::move(callContext),
            std::move(request));
```

In `GetService()`, pass the cell id:

```cpp
    IBlockStorePtr GetService() override
    {
        return std::make_shared<TControlService>(
            ControlRouter,
            shared_from_this(),
            Pool->GetCellId());
    }
```

- [ ] **Step 5: Run the test and the suite**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells`
Expected: PASS, whole suite GOOD.

- [ ] **Step 6: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/host_pool.h cloud/blockstore/libs/cells/impl/host_pool.cpp cloud/blockstore/libs/cells/impl/connection.cpp cloud/blockstore/libs/cells/impl/connection_ut.cpp
git commit -m "[Cells] Stamp CellId on forwarded Mount/Unmount"
```

---

### Task 2: TCellForwardService — the routing gate

**Files:**
- Create: `cloud/blockstore/libs/cells/iface/forward_service.h`
- Create: `cloud/blockstore/libs/cells/impl/forward_service.cpp`
- Modify: `cloud/blockstore/libs/cells/iface/ya.make`, `cloud/blockstore/libs/cells/impl/ya.make`
- Test: `cloud/blockstore/libs/cells/impl/forward_service_ut.cpp` (create) + `ya.make`

**Interfaces:**
- Produces:
```cpp
// cells/iface/forward_service.h
IBlockStorePtr CreateCellForwardService(
    IBlockStorePtr authorized,
    IBlockStorePtr trusted,
    IMonitoringServicePtr monitoring,
    ILoggingServicePtr logging);
```
- Consumes: nothing from earlier tasks (Task 1 is independent).

- [ ] **Step 1: Write the failing test.** Create `forward_service_ut.cpp`. A recording `IBlockStore` double counts calls and labels which of the two targets ran; a helper builds a request with a given source + cell id; assert the routing per the spec's test matrix.

```cpp
#include <cloud/blockstore/libs/cells/iface/forward_service.h>

#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

struct TTarget: public TTestService
{
    TString Name;
    ui32 Mounts = 0;

    explicit TTarget(TString name): Name(std::move(name))
    {
        MountVolumeHandler = [this] (auto) {
            ++Mounts;
            return MakeFuture(NProto::TMountVolumeResponse());
        };
    }
};

auto MakeEnv()
{
    struct TEnv
    {
        std::shared_ptr<TTarget> Authorized = std::make_shared<TTarget>("auth");
        std::shared_ptr<TTarget> Trusted = std::make_shared<TTarget>("trusted");
        IBlockStorePtr Service;

        TEnv()
        {
            Service = CreateCellForwardService(
                Authorized,
                Trusted,
                CreateMonitoringServiceStub(),
                CreateLoggingService("console"));
        }

        void Mount(NProto::ERequestSource source, const TString& cellId)
        {
            auto request = std::make_shared<NProto::TMountVolumeRequest>();
            auto& internal = *request->MutableHeaders()->MutableInternal();
            internal.SetRequestSource(source);
            if (cellId) {
                request->MutableHeaders()->SetCellId(cellId);
            }
            Service->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));
        }
    };
    return TEnv();
}

}   // namespace

Y_UNIT_TEST_SUITE(TCellForwardServiceTest)
{
    Y_UNIT_TEST(ShouldForwardTrustedInterCellMountToTrusted)
    {
        auto env = MakeEnv();
        env.Mount(NProto::SOURCE_SECURE_CONTROL_CHANNEL, "cell-1");
        UNIT_ASSERT_VALUES_EQUAL(1, env.Trusted->Mounts);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Authorized->Mounts);
    }

    Y_UNIT_TEST(ShouldAuthorizeWhenCellIdMissing)
    {
        auto env = MakeEnv();
        env.Mount(NProto::SOURCE_SECURE_CONTROL_CHANNEL, "");
        UNIT_ASSERT_VALUES_EQUAL(0, env.Trusted->Mounts);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Authorized->Mounts);
    }

    Y_UNIT_TEST(ShouldAuthorizeWhenSourceIsNotTrusted)
    {
        auto env = MakeEnv();
        env.Mount(NProto::SOURCE_INSECURE_CONTROL_CHANNEL, "cell-1");
        UNIT_ASSERT_VALUES_EQUAL(0, env.Trusted->Mounts);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Authorized->Mounts);
    }
}

}   // namespace NCloud::NBlockStore::NCells
```

(Adjust `TTestService`/`MountVolumeHandler` to the real names in `cloud/blockstore/libs/service/service_test.h`; `TabletHostToReport` in the cells test double shows the pattern.)

- [ ] **Step 2: Add build wiring.** In `cells/iface/ya.make` add `forward_service.cpp`… actually the factory is declared in iface and defined in impl, so add `forward_service.h` to iface `SRCS`/headers and `forward_service.cpp` to impl `SRCS`; add a `ya.make` `UNITTEST_FOR` entry for `forward_service_ut.cpp` next to the existing cells impl ut. Run the test and confirm it fails to link/compile (factory undefined).

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells`
Expected: FAIL — `CreateCellForwardService` unresolved.

- [ ] **Step 3: Implement the decorator** in `forward_service.cpp`:

```cpp
#include <cloud/blockstore/libs/cells/iface/forward_service.h>

#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NCells {

namespace {

bool IsWhitelisted(EBlockStoreRequest request)
{
    return request == EBlockStoreRequest::MountVolume
        || request == EBlockStoreRequest::UnmountVolume
        || request == EBlockStoreRequest::DescribeVolume;
}

bool IsTrustedSource(NCloud::NProto::ERequestSource source)
{
    // the single trusted server-stamped source for now; rdma adds its own
    // here when control starts flowing over it - a deliberate security
    // decision, never a side effect (see the design's decision 5)
    return source == NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL;
}

class TCellForwardService final
    : public TBlockStoreImpl<TCellForwardService, IBlockStore>
{
private:
    const IBlockStorePtr Authorized;
    const IBlockStorePtr Trusted;
    TLog Log;

public:
    TCellForwardService(
            IBlockStorePtr authorized,
            IBlockStorePtr trusted,
            ILoggingServicePtr logging)
        : Authorized(std::move(authorized))
        , Trusted(std::move(trusted))
    {
        Log = logging->CreateLog("BLOCKSTORE_CELLS");
    }

    void Start() override
    {
        // Authorized wraps the same inner stack Trusted points at, so
        // starting it starts everything once; starting Trusted too would
        // double-start the shared inner services
        Authorized->Start();
    }

    void Stop() override
    {
        Authorized->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Authorized->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        const auto& headers = request->GetHeaders();
        const bool whitelisted = IsWhitelisted(TMethod::BlockStoreRequest);
        const bool hasCellId = !headers.GetCellId().empty();
        const bool trustedSource =
            IsTrustedSource(headers.GetInternal().GetRequestSource());

        if (whitelisted && hasCellId && !trustedSource) {
            // the inter-cell marker on a channel we do not trust: a
            // misconfiguration or a forgery attempt. It is authorized like
            // any other request - the gate below is false - but it is worth
            // shouting about
            STORAGE_WARN(
                "[cell " << headers.GetCellId() << "] inter-cell marker from"
                    << " an untrusted source " << static_cast<int>(
                        headers.GetInternal().GetRequestSource())
                    << ", authorizing normally");
        }

        if (whitelisted && hasCellId && trustedSource) {
            return TMethod::Execute(
                Trusted.get(),
                std::move(callContext),
                std::move(request));
        }

        return TMethod::Execute(
            Authorized.get(),
            std::move(callContext),
            std::move(request));
    }
};

}   // namespace

IBlockStorePtr CreateCellForwardService(
    IBlockStorePtr authorized,
    IBlockStorePtr trusted,
    IMonitoringServicePtr monitoring,
    ILoggingServicePtr logging)
{
    Y_UNUSED(monitoring);   // used by Task 3
    return std::make_shared<TCellForwardService>(
        std::move(authorized),
        std::move(trusted),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore::NCells
```

- [ ] **Step 4: Run the tests**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells`
Expected: all three routing tests PASS.

- [ ] **Step 5: Commit**

```bash
git add cloud/blockstore/libs/cells/iface/forward_service.h cloud/blockstore/libs/cells/impl/forward_service.cpp cloud/blockstore/libs/cells/impl/forward_service_ut.cpp cloud/blockstore/libs/cells/iface/ya.make cloud/blockstore/libs/cells/impl/ya.make
git commit -m "[Cells] Forward service: route inter-cell control past authorization"
```

---

### Task 3: Inbound activity table + audit log + mon page

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/forward_service.cpp`
- Test: `cloud/blockstore/libs/cells/impl/forward_service_ut.cpp`

**Interfaces:**
- Consumes: `TCellForwardService` from Task 2, `IMonitoringServicePtr monitoring` already threaded into the factory.
- Produces: an internal `TActivity` table keyed by `(cellId, peer, diskId, clientId)` with `LastSeen` and per-method counts, rendered on a `TIndexMonPage` sub-page named `"CellInbound"`, plus an audit `STORAGE_INFO` on every trusted (authorization-skipped) request.

- [ ] **Step 1: Write the failing test** — a snapshot accessor is internal, so test through the mon page's HTML or add a test-only `SnapshotActivity()` on the concrete type. Simplest observable: after a trusted mount, the audit log and a snapshot include the source. Add a narrow test hook — a free function in the impl `namespace` `TVector<TString> DebugSnapshotActivityKeys(const IBlockStorePtr&)` declared in a test-only header is overkill; instead assert on the mon page output:

```cpp
    Y_UNIT_TEST(ShouldRecordInboundActivity)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto env = MakeEnv(monitoring);   // MakeEnv overload taking monitoring

        env.MountFrom(
            NProto::SOURCE_SECURE_CONTROL_CHANNEL,
            "cell-7",
            "disk-42",
            "client-3");

        auto page = RenderMonPage(monitoring, "blockstore", "CellInbound");
        UNIT_ASSERT_STRING_CONTAINS(page, "cell-7");
        UNIT_ASSERT_STRING_CONTAINS(page, "disk-42");
    }
```

Provide `RenderMonPage` as a small test helper that walks the monitoring stub's registered pages and calls `OutputContent` into a `TStringStream` (mirror how storage/service mon tests render). If the monitoring stub does not expose registered pages, thread a concrete `TDynamicCountersPtr`/page registry the test can read; check `cloud/storage/core/libs/diagnostics/monitoring.h` for the stub's surface and adapt.

- [ ] **Step 2: Run it and confirm it fails** (no page registered yet / no activity recorded).

- [ ] **Step 3: Implement.** Add to `TCellForwardService`:

```cpp
    struct TActivity
    {
        TString CellId;
        TString Peer;
        TString DiskId;
        TString ClientId;
        TInstant LastSeen;
        ui64 Mounts = 0;
        ui64 Unmounts = 0;
        ui64 Describes = 0;
    };

    ITimerPtr Timer;
    TAdaptiveLock Lock;
    THashMap<TString, TActivity> Activity;   // key: cell|peer|disk|client
```

A `RecordActivity(request, method)` called on the trusted branch updates the row under `Lock` (`LastSeen = Timer->Now()`, bump the per-method counter), and emits the audit line:

```cpp
        STORAGE_INFO(
            "[cell " << cellId << "] inter-cell " << GetBlockStoreRequestName(
                TMethod::BlockStoreRequest) << " without authorization"
                << ", peer=" << peer << " disk=" << diskId);
```

`SnapshotActivity()` copies rows whose `LastSeen` is within a TTL (a fixed `TDuration`, e.g. `TDuration::Minutes(5)`, pruning older rows under the lock) and returns them sorted for the page. Register a `THtmlMonPage` under the blockstore index (same mechanism as `TCellsMonPage` in `cell_manager_impl.cpp:65-69`) whose `OutputContent` renders the snapshot as a table (CellId, peer, disk, client, last seen, counts). Store the `IMonitoringServicePtr` and register in the ctor; add `ITimerPtr` to the factory or take it from an existing dependency — **Note:** the current factory signature has no timer; add `ITimerPtr timer` to `CreateCellForwardService` and thread it from the daemon (Task 4). Update Task 2's factory and the ut env accordingly.

- [ ] **Step 4: Run the tests** — routing tests still PASS, activity test PASS.

- [ ] **Step 5: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/forward_service.cpp cloud/blockstore/libs/cells/impl/forward_service_ut.cpp cloud/blockstore/libs/cells/iface/forward_service.h
git commit -m "[Cells] Forward service: inbound activity page and audit log"
```

---

### Task 4: Wire the forward service into the daemon

**Files:**
- Modify: `cloud/blockstore/libs/daemon/common/bootstrap.cpp` (around `InitAuthService`, ~740)
- Modify: `cloud/blockstore/libs/daemon/common/ya.make` if a new dep on `cells/iface` is needed (it already depends on it)

**Interfaces:**
- Consumes: `CreateCellForwardService(authorized, trusted, monitoring, logging, timer)` from Task 3.

- [ ] **Step 1: Capture the pre-auth service and wrap after auth.** In `bootstrap.cpp`, mirror the `udsService` capture (`:726`) but for the authorized path:

```cpp
    auto trusted = Service;                 // below auth
    InitAuthService();                      // Service := AuthService(trusted)

    if (Configs->CellsConfig->GetCellsEnabled()) {
        Service = NCells::CreateCellForwardService(
            Service,                        // authorized (through auth)
            trusted,
            Monitoring,
            Logging,
            Timer);
    }
```

`InitAuthService()` is a no-op when there is no actor system (`bootstrap.cpp` ydb variant guards it); the wrap must be conditional on cells being enabled so a cells-off node's stack is unchanged. Confirm `Timer`/`Monitoring`/`Logging` members exist on the bootstrap at this point (they are used nearby).

- [ ] **Step 2: Build the daemon lib**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -j 20 cloud/blockstore/libs/daemon/common`
Expected: Ok.

- [ ] **Step 3: Guard test — cells disabled leaves the stack unchanged.** If `daemon/common` has a bootstrap test, add one asserting that with `CellsEnabled=false` the `Service` identity is the auth service (no wrap). If there is no such harness, this is covered by the `GetCellsEnabled()` guard and verified by the integration smoke; note it in the commit and move on.

- [ ] **Step 4: Commit**

```bash
git add cloud/blockstore/libs/daemon/common/bootstrap.cpp
git commit -m "[Cells] Wire the inter-cell forward service into the service stack"
```

---

### Task 5: Manual/integration verification of the auth bypass

**Files:** none (verification task).

- [ ] **Step 1** Run the cells and daemon suites together:

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells cloud/blockstore/libs/daemon/common`
Expected: all GOOD.

- [ ] **Step 2** Grep the whole tree to confirm the only place that skips authorization for these methods is the forward service:

Run: `grep -rn "SOURCE_SECURE_CONTROL_CHANNEL\|CreateCellForwardService" cloud/blockstore/libs`
Expected: the forward service and its wiring are the only new hits; no other code keys authorization on `CellId`.

- [ ] **Step 3** Write the security assumption into the code where the gate lives (a comment block at the top of `forward_service.cpp`): the whole bypass rests on "the secure control port is reachable only from the underlay". Commit if the comment was added.

```bash
git add cloud/blockstore/libs/cells/impl/forward_service.cpp
git commit -m "[Cells] Document the forward service trust assumption"
```

---

### Task 6: Cells mon page — config dump

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/cell_manager_impl.cpp` (`TCellManager::OutputHtml`, currently a stub at `:181`)
- Test: `cloud/blockstore/libs/cells/impl/cell_manager_ut.cpp` if one exists, else a focused render test

**Interfaces:**
- Consumes: `TCellsConfig` via `Config->GetCells()`, `Config->GetGrpcClientConfig()`.

- [ ] **Step 1: Write the failing test** — render `OutputHtml` and assert it contains a configured cell id and host. If there is no cell_manager ut, add one that builds a `TCellManager` with a two-host config and calls `OutputHtml` into a `TStringStream` through a fake `IMonHttpRequest` (look at `service_actor_monitoring` tests for the request fake), asserting the html names a cell and a host.

- [ ] **Step 2: Run it and confirm it fails** — `OutputHtml` is a `Y_UNUSED` stub, output empty.

- [ ] **Step 3: Implement the config dump.** Replace the stub body with a table over `Config->GetCells()`: for each cell, its id, and for each host its fqdn and ports (`GetGrpcPort`/`GetSecureGrpcPort`/`GetRdmaPort`) and the flags (`GetHostMigrationEnabled`, `GetGrpcDataFallbackEnabled`, `GetHostPingPeriod`, `GetHostPingTimeout`). Use `library/cpp/monlib/service/pages/templates.h` `TABLE`/`TABLER`/`TABLED` macros (as `service_actor_monitoring.cpp` does). Keep it a static section — no actions yet.

- [ ] **Step 4: Run the test** — PASS.

- [ ] **Step 5: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/cell_manager_impl.cpp cloud/blockstore/libs/cells/impl/cell_manager_ut.cpp
git commit -m "[Cells] Mon page: dump the cells config"
```

---

### Task 7: Cells mon page — disk search

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/cell_manager_impl.cpp` (`OutputHtml` action dispatch + search handler)
- Test: `cloud/blockstore/libs/cells/impl/cell_manager_ut.cpp`

**Interfaces:**
- Consumes: `TCellManager::DescribeVolume(callContext, diskId, headers, service, clientConfig)` (`cell_manager.h:36`), which returns `TDescribeVolumeFuture` whose response carries `GetCellId()`.

- [ ] **Step 1: Write the failing test** — post `action=search&Volume=<diskId>` through the fake `IMonHttpRequest` and assert the rendered html names the cell the (faked) describe resolves to; a second case with an unknown disk asserts the page shows `not found` and does not hang or crash.

- [ ] **Step 2: Run it and confirm it fails** — no search handling yet.

- [ ] **Step 3: Implement.** In `OutputHtml`, read `request.GetParams()`/`GetPostParams()` for `action` and `Volume` (mirror `service_actor_monitoring.cpp:96-138`). Render a search form (`action=search`, `name=Volume`) as a section. When `action == "search"` and `Volume` is set, call `DescribeVolume(...)`, wait on the future with a timeout drawn from `GetGrpcClientConfig().GetRequestTimeout()` (`future.Wait(timeout)`), and render: on success the `GetCellId()`/host, on timeout an error line, on `HasError`/empty a `not found`. Because the page renders synchronously, blocking on the future with a bounded wait is intended here; never block unbounded.

- [ ] **Step 4: Run the tests** — search and config tests PASS.

- [ ] **Step 5: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/cell_manager_impl.cpp cloud/blockstore/libs/cells/impl/cell_manager_ut.cpp
git commit -m "[Cells] Mon page: search for a disk across cells"
```

---

## Self-review notes

- **Spec coverage:** decision 1–3 gate → Task 2; decision 5 (multi-source, rdma later) → `IsTrustedSource` comment in Task 2; decision 4 (decorator in cells, daemon only instantiates) → Tasks 2+4; CellId stamping → Task 1; daemon wiring outside auth, cells-gated → Task 4; audit log → Task 3; activity table (last-seen, TTL, no strict pairing) → Task 3; inbound page → Task 3; config dump → Task 6; disk search → Task 7; security assumption written down → Task 5. The "both transports" note (rdma server endpoint dispatches into the same service) is future work triggered when control-over-rdma lands — no task now, but the trust set is already a set, so nothing here blocks it.
- **Open detail for the executor:** the exact surface of the monitoring stub for rendering a registered page in a unit test (Tasks 3, 6, 7). If the stub does not let a test reach the registered page, render `OutputContent` directly on the concrete page object the code constructs, or add a thin test-only accessor. Resolve by reading `cloud/blockstore/libs/storage/service/service_actor_monitoring*` tests, which already render mon pages under test.
- **Timer thread-through:** Task 3 adds `ITimerPtr` to `CreateCellForwardService`; Task 2's signature and the ut env must be updated when Task 3 lands (the factory grows one argument). Executors doing Task 2 in isolation should include the timer parameter from the start to avoid a churn.
