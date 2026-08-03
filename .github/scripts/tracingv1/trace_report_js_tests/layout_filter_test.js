"use strict";

require("../trace_report_js_testing.js");

function testNameColumnWidthIsClampedToAUsableLayout() {
  assert.strictEqual(clampNameColumnWidth(100, 1200), 240);
  assert.strictEqual(clampNameColumnWidth(500, 1200), 500);
  assert.strictEqual(clampNameColumnWidth(2000, 1200), 800);
  assert.strictEqual(clampNameColumnWidth(Number.NaN, 1200), 240);
  assert.strictEqual(clampNameColumnWidth(500, 500), 240);
}

function testNameColumnWidthPersistenceHasSafeFallbacks() {
  const values = new Map([[NAME_COLUMN_STORAGE_KEY, "472"]]);
  const storage = {
    getItem(key) {
      return values.get(key) ?? null;
    },
    setItem(key, value) {
      values.set(key, value);
    },
  };

  assert.strictEqual(readStoredNameColumnWidth(storage), 472);
  assert.strictEqual(writeStoredNameColumnWidth(storage, 536), true);
  assert.strictEqual(values.get(NAME_COLUMN_STORAGE_KEY), "536");

  values.set(NAME_COLUMN_STORAGE_KEY, "not-a-width");
  assert.strictEqual(readStoredNameColumnWidth(storage), null);
  assert.strictEqual(readStoredNameColumnWidth(null), null);

  const blockedStorage = {
    getItem() {
      throw new Error("storage is blocked");
    },
    setItem() {
      throw new Error("storage is blocked");
    },
  };
  assert.strictEqual(readStoredNameColumnWidth(blockedStorage), null);
  assert.strictEqual(writeStoredNameColumnWidth(blockedStorage, 536), false);
}

function testCustomMinimumDurationUsesSeconds() {
  assert.strictEqual(parseMinimumDurationNs(""), null);
  assert.strictEqual(parseMinimumDurationNs("not-a-number"), null);
  assert.strictEqual(parseMinimumDurationNs("-1"), null);
  assert.strictEqual(parseMinimumDurationNs("0"), 0);
  assert.strictEqual(parseMinimumDurationNs("0.1"), 100_000_000);
  assert.strictEqual(parseMinimumDurationNs("600"), 600_000_000_000);
}

function testFilterClearStateUsesRawValuesAndPreservesDisplayControls() {
  assert.deepStrictEqual(filterControlActivity(), {
    query: false,
    minimumDuration: false,
    any: false,
  });
  assert.deepStrictEqual(
    filterControlActivity({ query: "   ", minimumDurationValue: "0" }),
    { query: true, minimumDuration: true, any: true },
  );
  assert.deepStrictEqual(
    filterControlActivity({ minimumDurationValue: "not-a-number" }),
    { query: false, minimumDuration: true, any: true },
  );
  assert.deepStrictEqual(
    filterControlActivity({ minimumDurationBadInput: true }),
    { query: false, minimumDuration: true, any: true },
  );
  for (const activeState of [
    { failedOnly: true },
    { topTestsOnly: true },
    { testPhaseValue: '["ya.test.stage",null]' },
    { testSizes: new Set(["small"]) },
  ]) {
    assert.strictEqual(filterControlActivity(activeState).any, true);
  }

  const original = {
    query: "local-endpoints",
    failedOnly: true,
    topTestsOnly: true,
    minimumDurationValue: "15",
    minimumDurationBadInput: false,
    testPhaseValue: '["ya.test.stage","prepare_recipes"]',
    testSizes: new Set(["medium", "large"]),
    timelineMode: "global",
    rowLoadSize: "5000",
    rowBudget: 5000,
  };
  const cleared = clearedFilterControlState(original);
  assert.deepStrictEqual(cleared, {
    query: "",
    failedOnly: false,
    topTestsOnly: false,
    minimumDurationValue: "",
    minimumDurationBadInput: false,
    testPhaseValue: "",
    testSizes: new Set(),
    timelineMode: "global",
    rowLoadSize: "5000",
    rowBudget: 5000,
  });
  assert.strictEqual(original.query, "local-endpoints");
  assert.deepStrictEqual(original.testSizes, new Set(["medium", "large"]));
}

function testTraceIndexPrecomputesHierarchyScopesAndPhaseOwners() {
  const spans = [
    makeSpan({ id: "chunk", name: "chunk", scope: 0 }),
    makeSpan({ id: "stage", parent: 0, name: "prepare", scope: 1 }),
    makeSpan({ id: "test", parent: 1, name: "test", scope: 2 }),
  ];
  const index = new TraceIndex(spans, ["ya.chunk", "ya.test.stage", "ya.test"]);

  assert.deepStrictEqual(index.roots, [0]);
  assert.deepStrictEqual(index.children, [[1], [2], []]);
  assert.strictEqual(index.scope(1), "ya.test.stage");
  assert.strictEqual(index.phaseOwner(1, "ya.test.stage"), 0);
  assert.deepStrictEqual(index.ancestors(2), [2, 1, 0]);
}

function testTraceIndexBuildsPhaseOwnersWithoutRepeatedAncestorWalks() {
  let parentReads = 0;
  const spans = [];
  function addSpan(parent, scope) {
    const span = makeSpan({
      id: `span-${spans.length}`,
      parent,
      name: `span ${spans.length}`,
      scope,
    });
    Object.defineProperty(span, PARENT, {
      configurable: true,
      get() {
        parentReads += 1;
        return parent;
      },
    });
    spans.push(span);
    return spans.length - 1;
  }

  const chunk = addSpan(-1, 0);
  let parent = chunk;
  for (let depth = 0; depth < 80; depth += 1) parent = addSpan(parent, 2);
  const stages = Array.from({ length: 100 }, () => addSpan(parent, 1));

  const index = new TraceIndex(spans, [
    "ya.chunk",
    "ya.test.stage",
    "other",
  ]);

  assert.ok(parentReads <= spans.length * 2, `${parentReads} parent reads`);
  stages.forEach((stage) => {
    assert.strictEqual(index.phaseOwner(stage, "ya.test.stage"), chunk);
  });
}

function testFilterNormalizationIsSeparateAndDeterministic() {
  const normalized = normalizeFilterSpec({
    query: "  FAILED Test  ",
    failedOnly: 1,
    minimumDurationNs: -1,
    testSizes: new Set(["SMALL", "unknown"]),
    testPhase: JSON.stringify(["ya.test.stage", "prepare_recipes"]),
    scopes: ["ya.test.stage"],
  });

  assert.strictEqual(normalized.query, "failed test");
  assert.strictEqual(normalized.failedOnly, true);
  assert.strictEqual(normalized.minimumDurationNs, null);
  assert.deepStrictEqual([...normalized.testSizes], ["small"]);
  assert.deepStrictEqual(normalized.testPhase, {
    scope: "ya.test.stage",
    name: "prepare_recipes",
  });
  assert.strictEqual(normalized.active, true);
  assert.strictEqual(normalized.hasSelection, true);
}

function testFailureFilterKeepsAncestorsButNotAggregateDescendants() {
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({
      id: "failed-chunk",
      parent: 0,
      name: "failed chunk",
      status: 2,
    }),
    makeSpan({
      id: "passing-child",
      parent: 1,
      name: "passing child of failed chunk",
    }),
    makeSpan({
      id: "failed-test",
      parent: 0,
      name: "Suite::failed_test",
      status: 2,
      attributes: { "test.size": "medium" },
    }),
  ];

  const result = filterVisibility(spans, { failedOnly: true });
  assert.strictEqual(result.matches, 2);
  assert.deepStrictEqual([...result.visible], [1, 0, 3]);
  assert.strictEqual(result.visible.has(2), false);
}

function testTextSearchDescendantsDoNotBypassComposedFilters() {
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({
      id: "failed-target",
      parent: 0,
      name: "cloud/tasks/storage failed target",
      status: 2,
      duration: 700_000_000_000,
      attributes: { "test.size": "medium" },
    }),
    makeSpan({
      id: "passing-child",
      parent: 1,
      name: "passing child",
      duration: 100_000_000_000,
      attributes: { "test.size": "small" },
    }),
  ];

  const failed = filterVisibility(spans, {
    query: "cloud/tasks/storage",
    failedOnly: true,
  });
  assert.strictEqual(failed.matches, 1);
  assert.deepStrictEqual([...failed.visible], [1, 0]);

  const longMedium = filterVisibility(spans, {
    query: "cloud/tasks/storage",
    minimumDurationNs: parseMinimumDurationNs("600"),
    testSizes: new Set(["medium"]),
  });
  assert.strictEqual(longMedium.matches, 1);
  assert.deepStrictEqual([...longMedium.visible], [1, 0]);
}

function testFiltersComposeByIntersection() {
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({
      id: "wanted",
      parent: 0,
      name: "StorageSuite::slow_failure",
      status: 2,
      duration: 700_000_000_000,
      attributes: {
        "test.size": "medium",
        "test.target": "cloud/tasks/storage",
      },
    }),
    makeSpan({
      id: "too-short",
      parent: 0,
      name: "StorageSuite::short_failure",
      status: 2,
      duration: 500_000_000_000,
      attributes: {
        "test.size": "medium",
        "test.target": "cloud/tasks/storage",
      },
    }),
    makeSpan({
      id: "wrong-size",
      parent: 0,
      name: "StorageSuite::slow_failure",
      status: 2,
      duration: 800_000_000_000,
      attributes: {
        "test.size": "small",
        "test.target": "cloud/tasks/storage",
      },
    }),
    makeSpan({
      id: "passing",
      parent: 0,
      name: "StorageSuite::slow_failure",
      duration: 900_000_000_000,
      attributes: {
        "test.size": "medium",
        "test.target": "cloud/tasks/storage",
      },
    }),
  ];

  const result = filterVisibility(spans, {
    query: "test.target=cloud/tasks/storage",
    failedOnly: true,
    minimumDurationNs: parseMinimumDurationNs("600"),
    testSizes: new Set(["medium"]),
  });
  assert.strictEqual(result.matches, 1);
  assert.deepStrictEqual([...result.visible], [1, 0]);
}

function testTopTenTestsUseProducerRank() {
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({
      id: "first",
      parent: 0,
      name: "first longest",
      attributes: { "ya.test.duration.rank": 1 },
    }),
    makeSpan({
      id: "tenth",
      parent: 0,
      name: "tenth longest",
      attributes: { "ya.test.duration.rank": "10" },
    }),
    makeSpan({
      id: "eleventh",
      parent: 0,
      name: "eleventh longest",
      attributes: { "ya.test.duration.rank": 11 },
    }),
    makeSpan({ id: "unranked", parent: 0, name: "unranked" }),
  ];

  assert.strictEqual(longestTestRank(spans[1]), 1);
  assert.strictEqual(longestTestRank(spans[2]), 10);
  assert.strictEqual(longestTestRank(spans[3]), null);
  assert.strictEqual(longestTestRank(spans[4]), null);

  const result = filterVisibility(spans, { topTestsOnly: true });
  assert.strictEqual(result.matches, 2);
  assert.deepStrictEqual([...result.visible], [1, 0, 2]);
}

function testSizeFiltersAreInclusiveAndIgnoreCase() {
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({
      id: "small",
      parent: 0,
      name: "small test",
      attributes: { "test.size": "SMALL" },
    }),
    makeSpan({
      id: "medium",
      parent: 0,
      name: "medium test",
      attributes: { "test.size": "medium" },
    }),
    makeSpan({
      id: "large",
      parent: 0,
      name: "large test",
      attributes: { "test.size": "large" },
    }),
  ];

  const result = filterVisibility(spans, {
    testSizes: new Set(["small", "large"]),
  });
  assert.strictEqual(result.matches, 2);
  assert.deepStrictEqual([...result.visible], [1, 0, 3]);
}

for (const test of [
  testNameColumnWidthIsClampedToAUsableLayout,
  testNameColumnWidthPersistenceHasSafeFallbacks,
  testCustomMinimumDurationUsesSeconds,
  testFilterClearStateUsesRawValuesAndPreservesDisplayControls,
  testTraceIndexPrecomputesHierarchyScopesAndPhaseOwners,
  testTraceIndexBuildsPhaseOwnersWithoutRepeatedAncestorWalks,
  testFilterNormalizationIsSeparateAndDeterministic,
  testFailureFilterKeepsAncestorsButNotAggregateDescendants,
  testTextSearchDescendantsDoNotBypassComposedFilters,
  testFiltersComposeByIntersection,
  testTopTenTestsUseProducerRank,
  testSizeFiltersAreInclusiveAndIgnoreCase,
]) {
  test();
}
