"use strict";

require("../trace_report_js_testing.js");

function testLoadSizesAreProgressiveIncrements() {
  assert.deepStrictEqual(LOAD_SIZE_OPTIONS, [200, 1000, 5000]);
  assert.strictEqual(INITIAL_ROW_BUDGET, 200);
  assert.strictEqual(loadSizeFromValue("200"), 200);
  assert.strictEqual(loadSizeFromValue("1000"), 1000);
  assert.strictEqual(loadSizeFromValue("5000"), 5000);
  assert.strictEqual(loadSizeFromValue("invalid"), 200);

  let rowBudget = INITIAL_ROW_BUDGET;
  const selectedLoadSize = loadSizeFromValue("1000");
  assert.strictEqual(rowBudget, 200);
  rowBudget = nextRowLimit(rowBudget, selectedLoadSize);
  assert.strictEqual(rowBudget, 1200);
  rowBudget = nextRowLimit(rowBudget, selectedLoadSize);
  assert.strictEqual(rowBudget, 2200);
  rowBudget = nextRowLimit(rowBudget, 5000);
  assert.strictEqual(rowBudget, 7200);
  assert.strictEqual(nextRowLimit(6400, 5000, 6500), 6500);
  assert.deepStrictEqual(groupLoadPlan(200, 6505, 1000), {
    count: 1000,
    nextLimit: 1200,
    remaining: 6305,
  });
  assert.deepStrictEqual(groupLoadPlan(6200, 6505, 5000), {
    count: 305,
    nextLimit: 6505,
    remaining: 305,
  });
}

function testChildCountsAndCriticalPathMarker() {
  assert.strictEqual(childCountLabel(1, "ya.chunk"), "1 test");
  assert.strictEqual(childCountLabel(12, "ya.chunk"), "12 tests");
  assert.strictEqual(childCountLabel(2, "ya.build"), "2 operations");
  assert.strictEqual(childCountLabel(3, "ya.phase"), "3 items");

  assert.strictEqual(
    isCriticalPathTest(
      makeSpan({
        id: "critical",
        name: "critical test",
        attributes: { "ya.test.critical_path": true },
      }),
    ),
    true,
  );
  assert.strictEqual(
    isCriticalPathTest(makeSpan({ id: "regular", name: "regular test" })),
    false,
  );
  assert.deepStrictEqual(
    criticalPathBadge(
      makeSpan({
        id: "inferred",
        name: "inferred test",
        attributes: {
          "ya.test.critical_path": true,
          "ya.test.critical_path.inferred": true,
          "ya.test.critical_path.granularity": "test-chunk",
        },
      }),
      "ya.test",
    ),
    {
      label: "★ in critical chunk",
      title:
        "The containing test chunk is on the ya critical path; ya does not identify this individual test as critical",
    },
  );
  assert.deepStrictEqual(
    criticalPathBadge(
      makeSpan({
        id: "chunk",
        name: "critical chunk",
        attributes: { "ya.test.critical_path": true },
      }),
      "ya.chunk",
    ),
    {
      label: "★ critical",
      title: "Test chunk is on the ya critical path",
    },
  );
}

function testChunkChildCountExcludesPreparationStages() {
  const scopes = [
    "ya.chunk",
    "ya.test",
    "ya.test.stage",
    "ya.test.operations",
    "ya.suite",
    "ya.test.node",
    "ya.test.worker",
    "ya.test.worker.phase",
  ];
  const spans = [
    makeSpan({ id: "chunk", name: "tests chunk", scope: 0 }),
    makeSpan({ id: "prepare", parent: 0, name: "prepare recipes", scope: 2 }),
    makeSpan({ id: "first", parent: 0, name: "Suite::first", scope: 1 }),
    makeSpan({ id: "second", parent: 0, name: "Suite::second", scope: 1 }),
    makeSpan({ id: "operations", name: "test operations", scope: 3 }),
    makeSpan({ id: "operation-chunk", parent: 4, name: "chunk", scope: 0 }),
    makeSpan({ id: "suite", parent: 4, name: "suite", scope: 4 }),
    makeSpan({ id: "unmatched", parent: 4, name: "worker", scope: 5 }),
    makeSpan({ id: "matched-worker", parent: 4, name: "worker", scope: 6 }),
    makeSpan({ id: "worker-chunk", parent: 8, name: "chunk", scope: 0 }),
    makeSpan({ id: "worker-phase", parent: 8, name: "setup", scope: 7 }),
    makeSpan({ id: "worker-test", parent: 9, name: "Suite::case", scope: 1 }),
    makeSpan({ id: "node-phase", parent: 7, name: "setup", scope: 7 }),
  ];
  const hierarchy = buildHierarchy(spans);

  assert.strictEqual(
    directChildCountLabel(0, spans, hierarchy.children, scopes),
    "2 tests",
  );
  assert.strictEqual(
    directChildCountLabel(1, spans, hierarchy.children, scopes),
    "0 items",
  );
  assert.strictEqual(
    directChildCountLabel(4, spans, hierarchy.children, scopes),
    "2 chunks · 1 other operation",
  );
  assert.strictEqual(
    directChildCountLabel(8, spans, hierarchy.children, scopes),
    "1 test · 1 phase",
  );
  assert.strictEqual(
    directChildCountLabel(7, spans, hierarchy.children, scopes),
    "1 phase",
  );
}

function testCollapsedGroupsAndExpansion() {
  const scopes = [
    "ya.run",
    "ya.build",
    "ya.build.node",
    "ya.chunk",
    "ya.test",
    "ya.test.operations",
    "ya.test.worker",
    "ya.test.node",
    "ya.test.worker.phase",
  ];
  const spans = [
    makeSpan({ id: "root", name: "root", scope: 0 }),
    makeSpan({ id: "build", parent: 0, name: "build operations", scope: 1 }),
    makeSpan({ id: "node", parent: 1, name: "compile target", scope: 2 }),
    makeSpan({ id: "chunk", parent: 0, name: "tests chunk 1/1", scope: 3 }),
    makeSpan({ id: "test", parent: 3, name: "Suite::test_case", scope: 4 }),
    makeSpan({
      id: "test-operations",
      parent: 0,
      name: "test operations",
      scope: 5,
    }),
    makeSpan({
      id: "operation-chunk",
      parent: 5,
      name: "operation tests chunk 1/1",
      scope: 3,
    }),
    makeSpan({ id: "test-worker", parent: 5, name: "test worker", scope: 6 }),
    makeSpan({ id: "setup", parent: 7, name: "setup", scope: 8 }),
    makeSpan({ id: "test-node", parent: 5, name: "aggregation", scope: 7 }),
    makeSpan({ id: "finalize", parent: 9, name: "finalize", scope: 8 }),
  ];
  const hierarchy = buildHierarchy(spans);
  assert.deepStrictEqual(hierarchy.roots, [0]);
  assert.deepStrictEqual(hierarchy.children, [
    [1, 3, 5],
    [2],
    [],
    [4],
    [],
    [6, 7, 9],
    [],
    [8],
    [],
    [10],
    [],
  ]);

  const expanded = defaultExpanded(
    spans,
    hierarchy.children,
    scopes,
    COLLAPSED_SCOPES,
  );
  assert.deepStrictEqual([...expanded], [0]);
  assert(COLLAPSED_SCOPES.has("ya.test.operations"));
  assert(COLLAPSED_SCOPES.has("ya.test.worker"));
  assert(COLLAPSED_SCOPES.has("ya.test.node"));
  const initial = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: expanded,
  });
  assert.deepStrictEqual(spanIndexes(initial), [0, 1, 3, 5]);

  expanded.add(1);
  const withBuild = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: expanded,
  });
  assert.deepStrictEqual(spanIndexes(withBuild), [0, 1, 2, 3, 5]);

  expanded.add(5);
  const withTestOperations = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: expanded,
  });
  assert.deepStrictEqual(
    spanIndexes(withTestOperations),
    [0, 1, 2, 3, 5, 6, 7, 9],
  );
}

function testSearchIncludesAttributesAndAncestors() {
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({ id: "chunk", parent: 0, name: "tests chunk" }),
    makeSpan({
      id: "test",
      parent: 1,
      name: "Suite::test_case",
      attributes: { "test.target": "cloud/tasks/storage" },
    }),
  ];
  const hierarchy = buildHierarchy(spans);
  const byName = matchingVisibility(spans, "TEST_CASE");
  assert.strictEqual(byName.matches, 1);
  assert.deepStrictEqual([...byName.visible], [2, 1, 0]);

  const byAttribute = matchingVisibility(spans, "test.target=cloud/tasks");
  assert.strictEqual(byAttribute.matches, 1);
  const filtered = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: new Set(),
    sourceVisible: byAttribute.visible,
  });
  assert.deepStrictEqual(spanIndexes(filtered), [0, 1, 2]);
}

function testSearchKeepsChildrenOfMatchingTestOperations() {
  const target = "cloud/blockstore/tests/loadtest/local-endpoints";
  const spans = [
    makeSpan({ id: "root", name: "test operations" }),
    makeSpan({ id: "unrelated", parent: 0, name: "unrelated operation" }),
    makeSpan({
      id: "aggregate",
      parent: 0,
      name: `test results aggregation: ${target} [py3test]`,
      attributes: { "test.suite": target },
    }),
    makeSpan({ id: "setup", parent: 2, name: "worker phase: setup" }),
    makeSpan({
      id: "exec",
      parent: 2,
      name: "worker phase: exec command",
    }),
  ];
  const hierarchy = buildHierarchy(spans);

  const result = matchingVisibility(spans, target);
  assert.strictEqual(result.matches, 1);
  const filtered = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: new Set(),
    sourceVisible: result.visible,
  });

  assert.deepStrictEqual(spanIndexes(filtered), [0, 2, 3, 4]);
}

for (const test of [
  testLoadSizesAreProgressiveIncrements,
  testChildCountsAndCriticalPathMarker,
  testChunkChildCountExcludesPreparationStages,
  testCollapsedGroupsAndExpansion,
  testSearchIncludesAttributesAndAncestors,
  testSearchKeepsChildrenOfMatchingTestOperations,
]) {
  test();
}
