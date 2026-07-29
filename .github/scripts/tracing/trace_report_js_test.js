"use strict";

const assert = require("assert");
const {
  FIELDS,
  COLLAPSED_SCOPES,
  LOAD_SIZE_OPTIONS,
  INITIAL_ROW_BUDGET,
  formatDuration,
  childCountLabel,
  isCriticalPathTest,
  buildHierarchy,
  defaultExpanded,
  matchingVisibility,
  flattenTraceRows,
  loadSizeFromValue,
  nextRowLimit,
  groupLoadPlan,
  initialGroupLimit,
} = require("./templates/trace_report.js");

function makeSpan({
  id,
  parent = -1,
  name,
  scope = 0,
  attributes = {},
  statusMessage = "",
}) {
  const span = Array(13).fill("");
  span[FIELDS.ID] = id;
  span[FIELDS.PARENT] = parent;
  span[FIELDS.NAME] = name;
  span[FIELDS.START] = 0;
  span[FIELDS.DURATION] = 1_000_000;
  span[FIELDS.ATTRS] = attributes;
  span[FIELDS.EVENTS] = [];
  span[FIELDS.STATUS] = 0;
  span[FIELDS.STATUS_MESSAGE] = statusMessage;
  span[FIELDS.RESOURCE] = 0;
  span[FIELDS.SCOPE] = scope;
  span[FIELDS.TRACE] = 0;
  span[FIELDS.ORPHAN_PARENT] = "";
  return span;
}

function spanIndexes(rows) {
  return rows.items
    .filter((item) => item.kind === "span")
    .map((item) => item.index);
}

function testDurationFormatting() {
  assert.strictEqual(formatDuration(0), "0 µs");
  assert.strictEqual(formatDuration(1_500_000), "1.5 ms");
  assert.strictEqual(formatDuration(2_500_000_000), "2.500 s");
  assert.strictEqual(formatDuration(65_000_000_000), "1m 5.0s");
  assert.strictEqual(formatDuration(3_665_000_000_000), "1h 1m 5s");
}

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
}

function testCollapsedGroupsAndExpansion() {
  const scopes = ["ya.run", "ya.build", "ya.build.node", "ya.chunk", "ya.test"];
  const spans = [
    makeSpan({ id: "root", name: "root", scope: 0 }),
    makeSpan({ id: "build", parent: 0, name: "build operations", scope: 1 }),
    makeSpan({ id: "node", parent: 1, name: "compile target", scope: 2 }),
    makeSpan({ id: "chunk", parent: 0, name: "tests chunk 1/1", scope: 3 }),
    makeSpan({ id: "test", parent: 3, name: "Suite::test_case", scope: 4 }),
  ];
  const hierarchy = buildHierarchy(spans);
  assert.deepStrictEqual(hierarchy.roots, [0]);
  assert.deepStrictEqual(hierarchy.children, [[1, 3], [2], [], [4], []]);

  const expanded = defaultExpanded(
    spans,
    hierarchy.children,
    scopes,
    COLLAPSED_SCOPES,
  );
  assert.deepStrictEqual([...expanded], [0]);
  const initial = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: expanded,
  });
  assert.deepStrictEqual(spanIndexes(initial), [0, 1, 3]);

  expanded.add(1);
  const withBuild = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: expanded,
  });
  assert.deepStrictEqual(spanIndexes(withBuild), [0, 1, 2, 3]);
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

function testGroupEntriesAreLoadedProgressively() {
  const spans = [makeSpan({ id: "root", name: "root" })];
  for (let index = 0; index < 6505; index += 1) {
    spans.push(
      makeSpan({
        id: `child-${index}`,
        parent: 0,
        name: `child ${index}`,
      }),
    );
  }
  const hierarchy = buildHierarchy(spans);
  const limits = new Map();
  let groupLimit = 200;
  for (const [loadSize, expectedChildren, hasMore] of [
    [0, 200, true],
    [1000, 1200, true],
    [5000, 6200, true],
    [5000, 6505, false],
  ]) {
    if (loadSize) {
      groupLimit = nextRowLimit(groupLimit, loadSize, 6505);
      limits.set(0, groupLimit);
    }
    const rows = flattenTraceRows({
      sourceSpans: spans,
      sourceChildren: hierarchy.children,
      sourceRoots: hierarchy.roots,
      sourceExpanded: new Set([0]),
      sourceLimits: limits,
      pageSize: 200,
      maximumRows: 7000,
    });
    assert.strictEqual(spanIndexes(rows).length, expectedChildren + 1);
    assert.strictEqual(
      rows.items.some((item) => item.kind === "more"),
      hasMore,
    );
  }
}

function testExpandedGroupUsesRemainingBudgetAndPreservesSuffix() {
  const spans = [
    makeSpan({ id: "workflow", name: "workflow" }),
    makeSpan({ id: "build", parent: 0, name: "ya make build" }),
    makeSpan({
      id: "build-operations",
      parent: 1,
      name: "build operations",
    }),
  ];
  const groupIndex = 2;
  for (let index = 0; index < 6505; index += 1) {
    spans.push(
      makeSpan({
        id: `operation-${index}`,
        parent: groupIndex,
        name: `operation ${index}`,
      }),
    );
  }
  const suffixIndexes = [];
  for (let index = 0; index < 79; index += 1) {
    suffixIndexes.push(spans.length);
    spans.push(
      makeSpan({
        id: `next-step-${index}`,
        parent: 0,
        name: `next GitHub step ${index}`,
      }),
    );
  }

  const hierarchy = buildHierarchy(spans);
  const collapsedExpanded = new Set([0, 1]);
  const collapsed = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: collapsedExpanded,
    maximumRows: INITIAL_ROW_BUDGET,
  });
  assert.strictEqual(collapsed.spanRows, 82);
  assert.strictEqual(collapsed.truncated, false);
  assert.strictEqual(spanIndexes(collapsed).at(-1), suffixIndexes.at(-1));

  const groupLimit = initialGroupLimit(
    INITIAL_ROW_BUDGET,
    collapsed.spanRows,
    hierarchy.children[groupIndex].length,
  );
  assert.strictEqual(groupLimit, 118);
  const expanded = new Set([...collapsedExpanded, groupIndex]);
  const initial = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: expanded,
    sourceLimits: new Map([[groupIndex, groupLimit]]),
    maximumRows: INITIAL_ROW_BUDGET,
  });
  assert.strictEqual(initial.spanRows, 200);
  assert.strictEqual(initial.truncated, false);
  assert.strictEqual(spanIndexes(initial).at(-1), suffixIndexes.at(-1));
  assert.strictEqual(
    initial.items.find((item) => item.kind === "more").shown,
    118,
  );

  const load = groupLoadPlan(groupLimit, 6505, 5000);
  const loaded = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: expanded,
    sourceLimits: new Map([[groupIndex, load.nextLimit]]),
    maximumRows: nextRowLimit(INITIAL_ROW_BUDGET, load.count),
  });
  assert.strictEqual(loaded.spanRows, 5200);
  assert.strictEqual(loaded.spanRows - initial.spanRows, 5000);
  assert.strictEqual(loaded.truncated, false);
  assert.strictEqual(spanIndexes(loaded).at(-1), suffixIndexes.at(-1));
  assert.strictEqual(
    loaded.items.find((item) => item.kind === "more").shown,
    5118,
  );
}

function testRenderLimitPreservesSiblingGroups() {
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({ id: "large", parent: 0, name: "large group" }),
  ];
  for (let index = 0; index < 1001; index += 1) {
    spans.push(
      makeSpan({
        id: `large-child-${index}`,
        parent: 1,
        name: `large child ${index}`,
      }),
    );
  }
  const siblingIndex = spans.length;
  spans.push(
    makeSpan({
      id: "sibling",
      parent: 0,
      name: "sibling group",
    }),
  );
  const siblingChildIndex = spans.length;
  spans.push(
    makeSpan({
      id: "sibling-child",
      parent: siblingIndex,
      name: "sibling child",
    }),
  );

  const hierarchy = buildHierarchy(spans);
  const rows = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: new Set([0, 1, siblingIndex]),
    pageSize: 200,
    maximumRows: 1000,
  });
  assert.strictEqual(rows.truncated, false);
  assert(spanIndexes(rows).includes(siblingIndex));
  assert(spanIndexes(rows).includes(siblingChildIndex));
  assert(
    rows.items.some(
      (item) =>
        item.kind === "more" &&
        item.index === 1 &&
        item.shown === 200 &&
        item.total === 1001,
    ),
  );
}

function testGlobalEntriesAreLoadedProgressively() {
  const spans = [];
  for (let index = 0; index < 6001; index += 1) {
    spans.push(makeSpan({ id: `root-${index}`, name: `root ${index}` }));
  }
  const hierarchy = buildHierarchy(spans);
  let rowBudget = INITIAL_ROW_BUDGET;
  for (const [loadSize, expected, truncated] of [
    [0, 200, true],
    [200, 400, true],
    [1000, 1400, true],
    [5000, 6001, false],
  ]) {
    if (loadSize) rowBudget = nextRowLimit(rowBudget, loadSize);
    const rows = flattenTraceRows({
      sourceSpans: spans,
      sourceChildren: hierarchy.children,
      sourceRoots: hierarchy.roots,
      sourceExpanded: new Set(),
      maximumRows: rowBudget,
    });
    assert.strictEqual(spanIndexes(rows).length, expected);
    assert.strictEqual(rows.truncated, truncated);
  }
}

function testPagingControlsDoNotConsumeTheRowBudget() {
  const spans = [makeSpan({ id: "root", name: "root" })];
  for (let index = 0; index < 201; index += 1) {
    spans.push(
      makeSpan({
        id: `child-${index}`,
        parent: 0,
        name: `child ${index}`,
      }),
    );
  }
  const hierarchy = buildHierarchy(spans);
  const rows = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: new Set([0]),
    pageSize: 199,
    maximumRows: 200,
  });
  assert.strictEqual(spanIndexes(rows).length, 200);
  assert.strictEqual(rows.items.length, 201);
  assert.strictEqual(rows.items.at(-1).kind, "more");
  assert.strictEqual(rows.truncated, false);
}

function testCyclesRemainReachable() {
  const spans = [
    makeSpan({ id: "left", parent: 1, name: "left" }),
    makeSpan({ id: "right", parent: 0, name: "right" }),
  ];
  const hierarchy = buildHierarchy(spans);
  assert.deepStrictEqual(hierarchy.roots, [0]);
  const rows = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: new Set([0, 1]),
  });
  assert.deepStrictEqual(spanIndexes(rows), [0, 1]);
}

for (const test of [
  testDurationFormatting,
  testLoadSizesAreProgressiveIncrements,
  testChildCountsAndCriticalPathMarker,
  testCollapsedGroupsAndExpansion,
  testSearchIncludesAttributesAndAncestors,
  testGroupEntriesAreLoadedProgressively,
  testExpandedGroupUsesRemainingBudgetAndPreservesSuffix,
  testRenderLimitPreservesSiblingGroups,
  testGlobalEntriesAreLoadedProgressively,
  testPagingControlsDoNotConsumeTheRowBudget,
  testCyclesRemainReachable,
]) {
  test();
}

console.log("All trace report JavaScript tests passed.");
