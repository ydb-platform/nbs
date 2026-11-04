"use strict";

const { loadTraceReportSource } = require("../trace_report_js_testing.js");
const vm = require("vm");

function testViewUrlRoundTripsEveryControl() {
  const view = {
    query: 'Suite::Тест + "setup & teardown" #1',
    failedOnly: true,
    topTestsOnly: true,
    minimumDurationNs: 125_000_000,
    testSizes: new Set(["small", "large"]),
    testPhase: encodeTestPhaseSelection("ya.test.worker"),
    order: encodeTestPhaseSelection("ya.test.stage", "prepare_recipes"),
    timelineMode: TIMELINE_MODES.LOCAL,
    rowLoadSize: 1000,
  };
  const url = new URL(traceViewUrl(
    "https://reports.example/trace.html?source=ci&tag=a&tag=b#page-bottom", view,
  ));
  assert.deepStrictEqual(readTraceView(url.search), view);
  assert.strictEqual(url.searchParams.get("phase"), "ya.test.worker");
  assert.strictEqual(url.searchParams.get("order"), "ya.test.stage/prepare_recipes");
  assert.strictEqual(url.searchParams.get("source"), "ci");
  assert.deepStrictEqual(url.searchParams.getAll("tag"), ["a", "b"]);
  assert.strictEqual(url.hash, "#page-bottom");
  assert.strictEqual(traceViewUrl(url.href, view), url.href);
}

function testClearingViewRemovesOwnedParametersAndKeepsTheReportUrl() {
  const original = "https://reports.example/run/trace.html?source=ci#page-top";
  const filtered = traceViewUrl(original, {
    query: "test", failedOnly: true, topTestsOnly: true,
    minimumDurationNs: 0, testSizes: ["medium"],
    testPhase: encodeTestPhaseSelection("ya.test"),
    order: ROW_ORDERS.DURATION, timelineMode: TIMELINE_MODES.LOCAL,
    rowLoadSize: 200,
  });
  assert.notStrictEqual(filtered, original);
  assert.strictEqual(traceViewUrl(filtered, readTraceView("")), original);
}

function testMalformedViewParametersFallBackToDefaults() {
  const defaults = readTraceView("");
  assert.deepStrictEqual(readTraceView(
    "?failed=0&top-tests=false&minimum-duration=-5&test-size=unknown" +
    "&phase=unknown/setup&order=ya.test/setup&timeline=unknown&rows=-1",
  ), defaults);
  for (const minimum of ["NaN", "Infinity", "1e309", "1e308", ""]) {
    assert.strictEqual(readTraceView(`?minimum-duration=${minimum}`).minimumDurationNs, null);
  }
  const flags = readTraceView("?failed&top-tests=true&minimum-duration=0");
  assert.strictEqual(flags.failedOnly, true);
  assert.strictEqual(flags.topTestsOnly, true);
  assert.strictEqual(flags.minimumDurationNs, 0);
  assert.deepStrictEqual(
    [...readTraceView("?test-size=small&test-size=small&test-size=bad").testSizes],
    ["small"],
  );
}

function testLocalLinksAndExactPhaseNamesRoundTrip() {
  for (const scope of ["ya.test.stage", "ya.test.worker.phase"]) {
    const testPhase = encodeTestPhaseSelection(scope, 'setup/with + & "quotes"');
    const href = traceViewUrl("file:///tmp/trace.html#page-bottom", {
      testPhase, order: testPhase, query: "a+b & c",
    });
    const view = readTraceView(new URL(href).search);
    assert.strictEqual(view.testPhase, testPhase);
    assert.strictEqual(view.order, testPhase);
    assert.strictEqual(view.query, "a+b & c");
    assert.strictEqual(new URL(href).hash, "#page-bottom");
  }
}

function makeViewRuntime(search, { blockHistory = false, traceModel = null } = {}) {
  const control = (name, value = "", type = "text") => ({
    name, value, type, checked: false, disabled: true, hidden: false,
    textContent: "", options: [],
    replaceChildren() { this.options = []; },
    append(option) { this.options.push(option); },
    focus() { this.focused = true; },
    select() { this.selected = true; },
  });
  const controls = {
    query: control("query"), minimum: control("minimum-duration"),
    phase: control("phase"), failed: control("failed", "on", "checkbox"),
    top: control("top-tests", "on", "checkbox"),
    size: control("test-size", "medium", "checkbox"),
    order: control("order", ROW_ORDERS.TREE),
    timeline: control("timeline", TIMELINE_MODES.GLOBAL),
    rows: control("rows", "5000"),
    share: control("share"), shareLink: control("share-link"),
  };
  controls.timeline.options = ["global", "local"].map((value) => ({ value }));
  controls.rows.options = ["200", "1000", "5000"].map((value) => ({ value }));
  const elements = [controls.query, controls.minimum, controls.phase,
    controls.failed, controls.top, controls.size];
  elements.namedItem = (name) => elements.find((element) => element.name === name);
  const testModel = traceModel || {
    c: ["ya.test.worker", "ya.chunk", "ya.test"],
    s: [
      makeSpan({ id: "worker", name: "target", duration: 10e9,
        attributes: { "test.size": "medium" } }),
      makeSpan({ id: "chunk", name: "chunk", parent: 0, scope: 1, duration: 8e9 }),
      makeSpan({ id: "test", name: "case", parent: 1, scope: 2, duration: 5e9 }),
    ],
  };
  const window = {
    location: new URL(`https://reports.example/trace.html${search}`),
    history: {
      state: { preserved: true },
      replaceState(state, _title, href) {
        if (blockHistory) throw new Error("history unavailable");
        this.state = state;
        window.location = new URL(href);
      },
    },
  };
  const context = vm.createContext({
    URL, URLSearchParams, window, controls, elements, control, testModel,
    module: { exports: {} }, navigator: {}, clearTimeout,
    document: { createElement: () => control("") },
    FormData: class extends URLSearchParams {
      constructor(form) {
        super(form.elements.filter((element) =>
          !element.disabled && (element.type !== "checkbox" || element.checked),
        ).map(({ name, value }) => [name, value]));
      }
    },
  });
  vm.runInContext(loadTraceReportSource(), context);
  vm.runInContext(`
    filterForm = { elements };
    filterElement = controls.query;
    minimumDurationElement = controls.minimum;
    testPhaseElement = controls.phase;
    rowOrderElement = controls.order;
    timelineModeElement = controls.timeline;
    rowLoadSizeElement = controls.rows;
    clearFilterElement = control("");
    clearMinimumDurationElement = control("");
    clearFiltersElement = control("");
    filterStatus = control("");
    expandElement = control("");
    collapseElement = control("");
    copyViewLinkElement = control("");
    shareLinkControl = controls.share;
    shareLinkElement = controls.shareLink;
    shareStatus = control("");
    renderRows = () => { syncTraceViewUrl(); globalThis.rendered = flattenRows(); };
    initialize(testModel);
  `, context);
  return context;
}

function testInitialViewRestoresDynamicChoicesBeforeFilteringAndOrdering() {
  const context = makeViewRuntime(
    "?query=target&phase=ya.test.worker&minimum-duration=9&test-size=medium" +
    "&order=ya.test.worker&timeline=local&rows=1000",
  );
  assert.strictEqual(context.controls.phase.value, encodeTestPhaseSelection("ya.test.worker"));
  assert.strictEqual(context.controls.order.value, context.controls.phase.value);
  assert.strictEqual(context.controls.timeline.value, "local");
  assert.strictEqual(context.controls.rows.value, "1000");
  assert.strictEqual(context.controls.size.checked, true);
  assert.strictEqual(context.rendered.items.length, 1);
  assert.strictEqual(context.rendered.items[0].index, 0);
  assert.strictEqual(context.window.history.state.preserved, true);

  context.window.location = new URL("https://reports.example/trace.html?query=missing");
  vm.runInContext("restoreTraceViewOnNavigation();", context);
  assert.strictEqual(context.rendered.items.length, 0);
  assert.strictEqual(context.controls.order.value, ROW_ORDERS.TREE);
  assert.strictEqual(context.controls.size.checked, false);
}

function testUnavailablePhasesFallBackWithoutBreakingTheReport() {
  const context = makeViewRuntime(
    "?phase=ya.test.stage/unknown&order=ya.test.worker.phase/unknown",
  );
  assert.strictEqual(context.controls.phase.value, "");
  assert.strictEqual(context.controls.order.value, ROW_ORDERS.TREE);
  assert(context.rendered.items.length > 0);
}

function testHashNavigationPreservesLoadedGroupsAndPendingInput() {
  const context = makeViewRuntime("");
  vm.runInContext(`
    rowBudget = 1200;
    limits.set(1, 1000);
    expanded.add(0);
    expanded.add(1);
    renderRows();
  `, context);
  const rendered = context.rendered;
  assert.strictEqual(rendered.spanRows, 3);
  context.controls.query.value = "pending input";
  for (const hash of ["#page-bottom", "#page-top", ""]) {
    context.window.location.hash = hash;
    vm.runInContext("restoreTraceViewOnNavigation();", context);
    assert.strictEqual(context.rendered, rendered);
    assert.strictEqual(vm.runInContext("rowBudget", context), 1200);
    assert.strictEqual(vm.runInContext("limits.get(1)", context), 1000);
    assert.strictEqual(vm.runInContext("expanded.has(0) && expanded.has(1)", context), true);
    assert.strictEqual(context.controls.query.value, "pending input");
  }
  // Actual filter navigation still restores controls and resets pagination.
  context.window.location.search = "?query=missing";
  vm.runInContext("restoreTraceViewOnNavigation();", context);
  assert.strictEqual(context.controls.query.value, "missing");
  assert.strictEqual(vm.runInContext("rowBudget", context), INITIAL_ROW_BUDGET);
  assert.strictEqual(vm.runInContext("limits.size", context), 0);
  assert.strictEqual(context.rendered.spanRows, 0);
}

function testHashNavigationPreservesFiltersAfterUrlSync() {
  for (const blockHistory of [false, true]) {
    const context = makeViewRuntime("", { blockHistory });
    context.controls.query.value = "case";
    vm.runInContext(`
      applyFilterImmediately();
      rowBudget = 1200;
      renderRows();
    `, context);
    const rendered = context.rendered;
    assert.strictEqual(context.window.location.search, blockHistory ? "" : "?query=case");
    context.window.location.hash = "#page-bottom";
    vm.runInContext("restoreTraceViewOnNavigation();", context);
    assert.strictEqual(context.rendered, rendered);
    assert.strictEqual(context.controls.query.value, "case");
    assert.strictEqual(vm.runInContext("rowBudget", context), 1200);
  }
}

function testSharedGroupedOrderKeepsTestNameMatches() {
  for (const [order, expected] of [
    ["ya.chunk", [1, 2]],
    ["ya.test.worker", [0, 1, 2]],
  ]) {
    const context = makeViewRuntime(`?query=case&order=${order}`);
    assert.deepStrictEqual(
      Array.from(context.rendered.items, ({ index }) => index),
      expected,
    );
  }
}

function testSharedFlatOrderExcludesContextRows() {
  const context = makeViewRuntime(
    "?phase=ya.test&minimum-duration=4&order=duration",
  );
  assert.deepStrictEqual(
    Array.from(context.rendered.items, ({ index }) => index),
    [2],
  );
}

function testSharedWorkerDurationKeepsContainedTestNameSearch() {
  const context = makeViewRuntime(
    "?query=case&phase=ya.test.worker&minimum-duration=9&order=ya.test",
  );
  assert.deepStrictEqual(
    Array.from(context.rendered.items, ({ index }) => index),
    [2],
  );
}

function testSharedWorkerDurationFilterCanOrderTestsWithLocalTimelines() {
  const traceModel = {
    c: ["ya.test.worker", "ya.chunk", "ya.test"],
    s: [
      makeSpan({ id: "slow-worker", name: "slow worker", duration: 90e9 }),
      makeSpan({ id: "slow-chunk", name: "chunk", parent: 0, scope: 1, duration: 80e9 }),
      makeSpan({ id: "short-test", name: "short test", parent: 1, scope: 2, duration: 5e9 }),
      makeSpan({ id: "fast-worker", name: "fast worker", duration: 10e9 }),
      makeSpan({ id: "fast-test", name: "another test", parent: 3, scope: 2, duration: 7e9 }),
    ],
  };
  const context = makeViewRuntime(
    "?minimum-duration=60&phase=ya.test.worker&order=ya.test&timeline=local",
    { traceModel },
  );
  assert.deepStrictEqual(
    Array.from(context.rendered.items, ({ index }) => index),
    [2],
  );
  assert.strictEqual(context.controls.minimum.value, "60");
  assert.strictEqual(context.controls.timeline.value, TIMELINE_MODES.LOCAL);
  const geometry = timelineBarGeometry(
    2, traceModel.s, traceModel.c, 90e9, context.controls.timeline.value,
  );
  assert.strictEqual(geometry.relativeTo, 1);
  assert.strictEqual(geometry.width, 6.25);
}

function testCopyFallbackWorksWhenHistoryAndClipboardAreUnavailable() {
  const context = makeViewRuntime("", { blockHistory: true });
  context.controls.query.value = "new filter + &";
  vm.runInContext("copyTraceViewLink();", context);
  assert.strictEqual(context.controls.share.hidden, false);
  assert.strictEqual(context.controls.shareLink.selected, true);
  assert.strictEqual(
    new URL(context.controls.shareLink.value).searchParams.get("query"),
    "new filter + &",
  );
  assert.strictEqual(context.window.location.search, "");
}

function testSharedWorkerDurationKeepsTestPredicates() {
  const traceModel = {
    c: ["ya.test.worker", "ya.test"],
    s: [
      makeSpan({ id: "worker", name: "worker", duration: 90e9, status: 2 }),
      makeSpan({ id: "failed", name: "failed case", parent: 0, scope: 1,
        duration: 5e9, status: 2, attributes: { "ya.test.duration.rank": 1 } }),
      makeSpan({ id: "passed", name: "passing sibling", parent: 0, scope: 1,
        duration: 7e9 }),
    ],
  };
  for (const predicate of ["failed=1", "top-tests=1", "query=failed%20case"]) {
    const context = makeViewRuntime(
      `?phase=ya.test.worker&minimum-duration=60&order=ya.test&${predicate}`,
      { traceModel },
    );
    assert.deepStrictEqual(
      Array.from(context.rendered.items, ({ index }) => index),
      [1],
    );
  }
  for (const order of ["tree", "duration", "ya.test.worker", "ya.test"]) {
    for (const predicate of ["failed=1", "top-tests=1"]) {
      const context = makeViewRuntime(
        `?query=passing&phase=ya.test.worker&minimum-duration=60&order=${order}&${predicate}`,
        { traceModel },
      );
      assert.strictEqual(context.rendered.spanRows, 0);
    }
  }
}

for (const test of [
  testViewUrlRoundTripsEveryControl,
  testClearingViewRemovesOwnedParametersAndKeepsTheReportUrl,
  testMalformedViewParametersFallBackToDefaults,
  testLocalLinksAndExactPhaseNamesRoundTrip,
  testInitialViewRestoresDynamicChoicesBeforeFilteringAndOrdering,
  testUnavailablePhasesFallBackWithoutBreakingTheReport,
  testHashNavigationPreservesLoadedGroupsAndPendingInput,
  testHashNavigationPreservesFiltersAfterUrlSync,
  testSharedGroupedOrderKeepsTestNameMatches,
  testSharedFlatOrderExcludesContextRows,
  testSharedWorkerDurationKeepsContainedTestNameSearch,
  testSharedWorkerDurationFilterCanOrderTestsWithLocalTimelines,
  testCopyFallbackWorksWhenHistoryAndClipboardAreUnavailable,
  testSharedWorkerDurationKeepsTestPredicates,
]) {
  test();
}
