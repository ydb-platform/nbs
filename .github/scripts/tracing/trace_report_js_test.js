"use strict";

const assert = require("assert");
const {
  FIELDS,
  COLLAPSED_SCOPES,
  RENDER_LIMIT_OPTIONS,
  INITIAL_RENDER_LIMIT,
  formatDuration,
  childCountLabel,
  isCriticalPathTest,
  buildHierarchy,
  defaultExpanded,
  matchingVisibility,
  flattenTraceRows,
  renderLimitFromValue,
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

function testRenderLimitOptions() {
  assert.deepStrictEqual(RENDER_LIMIT_OPTIONS, [200, 1000, 5000]);
  assert.strictEqual(INITIAL_RENDER_LIMIT, 200);
  assert.strictEqual(renderLimitFromValue("200"), 200);
  assert.strictEqual(renderLimitFromValue("1000"), 1000);
  assert.strictEqual(renderLimitFromValue("5000"), 5000);
  assert.strictEqual(
    renderLimitFromValue("all"),
    Number.POSITIVE_INFINITY,
  );
  assert.strictEqual(renderLimitFromValue("invalid"), 200);
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

function testPagingAndGlobalLimit() {
  const spans = [makeSpan({ id: "root", name: "root" })];
  for (let index = 0; index < 205; index += 1) {
    spans.push(
      makeSpan({
        id: `child-${index}`,
        parent: 0,
        name: `child ${index}`,
      }),
    );
  }
  const hierarchy = buildHierarchy(spans);
  const paged = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: new Set([0]),
    pageSize: 200,
    maximumRows: 1000,
  });
  assert.strictEqual(spanIndexes(paged).length, 201);
  assert.deepStrictEqual(paged.items.at(-1), {
    kind: "more",
    index: 0,
    depth: 1,
    total: 205,
    shown: 200,
  });

  const limited = flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: hierarchy.children,
    sourceRoots: hierarchy.roots,
    sourceExpanded: new Set([0]),
    maximumRows: 10,
  });
  assert.strictEqual(limited.truncated, true);
  assert.strictEqual(limited.items.length, 10);
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

function testConfiguredGlobalRowCaps() {
  const spans = [];
  for (let index = 0; index < 6001; index += 1) {
    spans.push(makeSpan({ id: `root-${index}`, name: `root ${index}` }));
  }
  const hierarchy = buildHierarchy(spans);
  for (const [limit, expected, truncated] of [
    [200, 200, true],
    [1000, 1000, true],
    [5000, 5000, true],
    [Number.POSITIVE_INFINITY, 6001, false],
  ]) {
    const rows = flattenTraceRows({
      sourceSpans: spans,
      sourceChildren: hierarchy.children,
      sourceRoots: hierarchy.roots,
      sourceExpanded: new Set(),
      maximumRows: limit,
    });
    assert.strictEqual(spanIndexes(rows).length, expected);
    assert.strictEqual(rows.truncated, truncated);
  }
}

function testGlobalLimitIncludesPagingControls() {
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
  assert.strictEqual(rows.items.length, 200);
  assert.strictEqual(rows.truncated, true);
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
  testRenderLimitOptions,
  testChildCountsAndCriticalPathMarker,
  testCollapsedGroupsAndExpansion,
  testSearchIncludesAttributesAndAncestors,
  testPagingAndGlobalLimit,
  testRenderLimitPreservesSiblingGroups,
  testConfiguredGlobalRowCaps,
  testGlobalLimitIncludesPagingControls,
  testCyclesRemainReachable,
]) {
  test();
}

console.log("All trace report JavaScript tests passed.");
