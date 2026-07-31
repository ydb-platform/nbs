"use strict";

require("../trace_report_js_testing.js");

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
  testGroupEntriesAreLoadedProgressively,
  testExpandedGroupUsesRemainingBudgetAndPreservesSuffix,
  testRenderLimitPreservesSiblingGroups,
  testGlobalEntriesAreLoadedProgressively,
  testPagingControlsDoNotConsumeTheRowBudget,
  testCyclesRemainReachable,
]) {
  test();
}

