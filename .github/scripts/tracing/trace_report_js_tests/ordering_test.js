"use strict";

require("../trace_report_js_testing.js");

const second = 1_000_000_000;
const scopes = [
  "workflow", "ya.test.worker", "ya.chunk", "ya.test",
  "ya.test.stage", "ya.test.worker.phase", "ya.test.node",
];
const medium = { "test.size": "medium" };
const small = { "test.size": "small" };
const spans = [
  makeSpan({ id: "root", name: "root", duration: 100 * second }),
  makeSpan({ id: "worker-a", parent: 0, name: "target A", scope: 1,
    duration: 30 * second, status: 2, attributes: medium }),
  makeSpan({ id: "setup-a", parent: 1, name: "setup", scope: 5,
    duration: 3 * second, attributes: { "ya.test.worker.phase": "setup" } }),
  makeSpan({ id: "chunk-a", parent: 1, name: "target A chunk", scope: 2,
    duration: 20 * second, status: 2, attributes: medium }),
  makeSpan({ id: "prepare-a", parent: 3, name: "prepare_recipes", scope: 4,
    duration: 8 * second, attributes: { "ya.test.stage.name": "prepare_recipes" } }),
  makeSpan({ id: "test-a", parent: 3, name: "case A", scope: 3,
    duration: 15 * second, status: 2,
    attributes: { ...medium, "test.duration.reported_seconds": 2,
      "ya.test.duration.rank": 1 } }),
  makeSpan({ id: "test-b", parent: 3, name: "case B", scope: 3,
    duration: second,
    attributes: { ...medium, "test.duration.reported_seconds": 10 } }),
  makeSpan({ id: "worker-b", parent: 0, name: "target B", scope: 1,
    duration: 25 * second, attributes: small }),
  makeSpan({ id: "setup-b", parent: 7, name: "setup", scope: 5,
    duration: 9 * second, attributes: { "ya.test.worker.phase": "setup" } }),
  makeSpan({ id: "chunk-b", parent: 7, name: "target B chunk", scope: 2,
    duration: 23 * second, attributes: small }),
  makeSpan({ id: "prepare-b", parent: 9, name: "prepare_recipes", scope: 4,
    duration: 4 * second, attributes: { "ya.test.stage.name": "prepare_recipes" } }),
  makeSpan({ id: "test-c", parent: 9, name: "case C", scope: 3,
    duration: 0,
    attributes: { ...small, "test.duration.reported_seconds": 3 } }),
  makeSpan({ id: "worker-c", parent: 0, name: "target C", scope: 1,
    duration: 30 * second, attributes: medium }),
  makeSpan({ id: "chunk-c", parent: 12, name: "target C chunk", scope: 2,
    duration: 28 * second, attributes: medium }),
  makeSpan({ id: "test-d", parent: 13, name: "case D", scope: 3,
    duration: 10 * second, attributes: medium }),
  makeSpan({ id: "prepare-c", parent: 13, name: "prepare_recipes", scope: 4,
    duration: 2 * second, attributes: { "ya.test.stage.name": "prepare_recipes" } }),
  makeSpan({ id: "support", parent: 0, name: "test support operation", scope: 6,
    duration: 99 * second }),
];
const index = new TraceIndex(spans, scopes);
const tests = encodeTestPhaseSelection("ya.test");
const chunks = encodeTestPhaseSelection("ya.chunk");
const workers = encodeTestPhaseSelection("ya.test.worker");
const prepare = encodeTestPhaseSelection("ya.test.stage", "prepare_recipes");
const setup = encodeTestPhaseSelection("ya.test.worker.phase", "setup");

function testSharedDurationChoicesIncludeTestsAndInfrastructure() {
  const options = testPhaseOptions(spans, scopes);
  for (const value of [tests, chunks, workers, prepare, setup]) {
    assert(options.some((option) => option.value === value));
    assert.strictEqual(rowOrderFromValue(value), value);
  }
  assert.strictEqual(options.filter(({ value }) => value === workers).length, 1);
  assert.strictEqual(rowOrderFromValue("unknown"), ROW_ORDERS.TREE);
  assert.strictEqual(rowOrderFromValue(ROW_ORDERS.DURATION), ROW_ORDERS.DURATION);
  assert.strictEqual(parseTestPhaseSelection('["ya.test", "setup"]'), null);
  assert.strictEqual(normalizeFilterSpec({
    testPhase: { scope: "ya.chunk", name: "setup" },
  }).testPhase, null);
  assert.strictEqual(rowOrderIsFlat(tests), true);
  assert.strictEqual(rowOrderIsFlat(prepare), true);
  assert.strictEqual(rowOrderIsFlat(chunks), false);
  assert.strictEqual(rowOrderIsFlat(workers), false);
  assert.strictEqual(rowOrderIsFlat(ROW_ORDERS.TREE), false);
}

function testOrderingUsesDisplayedDurationsAndStableTies() {
  const original = spans.map((span) => span.slice());
  assert.deepStrictEqual(durationOrderedIndexes(index, tests), [6, 14, 11, 5]);
  assert.deepStrictEqual(durationOrderedIndexes(index, chunks), [13, 9, 3]);
  assert.deepStrictEqual(durationOrderedIndexes(index, workers), [1, 12, 7]);
  assert.deepStrictEqual(durationOrderedIndexes(index, prepare), [4, 10, 15]);
  assert.deepStrictEqual(durationOrderedIndexes(index, setup), [8, 2]);
  assert.deepStrictEqual(spans, original);
}

function testFullWorkerDurationDoesNotSumOverlappingChildren() {
  const result = filterVisibility(spans, {
    testPhase: workers, minimumDurationNs: 29 * second, scopes,
  });
  assert.deepStrictEqual([...result.matching], [1, 12]);
  assert.strictEqual(result.matches, 2);
  assert.deepStrictEqual(durationOrderedIndexes(index, workers, {
    minimumDurationNs: 29 * second,
  }), [1, 12]);
  assert.deepStrictEqual(durationOrderedIndexes(index, chunks, {
    minimumDurationNs: 29 * second,
  }), []);
}

function testFilteringAndOrderingAgreeOnReportedTestDuration() {
  const result = filterVisibility(spans, {
    testPhase: tests, minimumDurationNs: 9 * second, scopes,
  });
  assert.deepStrictEqual([...result.matching], [6, 14]);
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, {
    minimumDurationNs: 9 * second,
  }), [6, 14]);
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, {
    failedOnly: true,
  }), [5]);
}

function testOrderedPhasesUseTheirOwnersForComposedFilters() {
  assert.deepStrictEqual(durationOrderedIndexes(index, prepare, {
    query: "target A", failedOnly: true, testSizes: new Set(["medium"]),
    minimumDurationNs: 5 * second,
  }), [4]);
  assert.deepStrictEqual(durationOrderedIndexes(index, setup, {
    failedOnly: true, testSizes: new Set(["small"]),
  }), []);
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, {
    query: "target A",
  }), [6, 5]);
  assert.deepStrictEqual(durationOrderedIndexes(index, workers, {
    topTestsOnly: true,
  }), [1]);
  assert.deepStrictEqual(durationOrderedIndexes(index, chunks, {
    topTestsOnly: true,
  }), [3]);
}

function testOrderedGroupsCanBeFilteredByAnotherDurationChoice() {
  const filters = { testPhase: prepare, minimumDurationNs: 5 * second };
  assert.deepStrictEqual(durationOrderedIndexes(index, chunks, filters), [3]);
  assert.deepStrictEqual(durationOrderedIndexes(index, workers, filters), [1]);
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, filters), []);
  assert.deepStrictEqual(durationOrderedIndexes(index, ROW_ORDERS.DURATION, {
    failedOnly: true, minimumDurationNs: 19 * second,
  }), [1, 3]);
}

function testOrderedGroupsContainMatchingTests() {
  for (const [order, expected] of [[chunks, [3]], [workers, [1]]]) {
    assert.deepStrictEqual(durationOrderedIndexes(index, order, {
      query: "case A",
    }), expected);
    assert.deepStrictEqual(durationOrderedIndexes(index, order, {
      query: "case A", failedOnly: true,
    }), expected);
    assert.deepStrictEqual(durationOrderedIndexes(index, order, {
      query: "case B", testSizes: new Set(["medium"]),
      minimumDurationNs: 9 * second,
    }), expected);
    // A failed sibling must not make a matching passing test qualify.
    assert.deepStrictEqual(durationOrderedIndexes(index, order, {
      query: "case B", failedOnly: true,
    }), []);
    assert.deepStrictEqual(durationOrderedIndexes(index, order, {
      query: "case A", minimumDurationNs: 5 * second,
    }), []);
    assert.deepStrictEqual(durationOrderedIndexes(index, order, {
      query: "missing case",
    }), []);
  }
  // Multiple matching tests in a group yield one group, in duration order.
  assert.deepStrictEqual(durationOrderedIndexes(index, chunks, {
    query: "case",
  }), [13, 9, 3]);
  assert.deepStrictEqual(durationOrderedIndexes(index, workers, {
    query: "case",
  }), [1, 12, 7]);
}

function testFlatOrderingExcludesAncestorsAndUnmatchedChildren() {
  assert.deepStrictEqual(durationOrderedIndexes(index, ROW_ORDERS.DURATION, {
    testPhase: workers, query: "target A", minimumDurationNs: 25 * second,
  }), [1]);
  const filters = { testPhase: prepare, minimumDurationNs: 5 * second };
  assert.deepStrictEqual(
    durationOrderedIndexes(index, ROW_ORDERS.DURATION, filters),
    [4],
  );
  // Ancestors remain available when explicitly ordering containing groups.
  assert.deepStrictEqual(durationOrderedIndexes(index, chunks, filters), [3]);
  assert.deepStrictEqual(durationOrderedIndexes(index, workers, filters), [1]);
  assert.deepStrictEqual(durationOrderedIndexes(index, ROW_ORDERS.DURATION, {
    query: "case A",
  }), [5]);
  assert.deepStrictEqual(durationOrderedIndexes(index, ROW_ORDERS.DURATION, {
    query: "target A",
  }), [1, 3]);
  assert.deepStrictEqual(durationOrderedIndexes(index, ROW_ORDERS.DURATION, {
    query: "case",
  }), [6, 14, 11, 5]);
  assert.deepStrictEqual(durationOrderedIndexes(index, ROW_ORDERS.DURATION, {
    query: "missing case",
  }), []);
  assert.strictEqual(
    durationOrderedIndexes(index, ROW_ORDERS.DURATION).length,
    spans.length,
  );
}

function testGroupDurationCanMatchContainedTestNames() {
  for (const [testPhase, expected] of [[workers, [1]], [chunks, [3]]]) {
    const filters = { testPhase, query: "case A", minimumDurationNs: 19 * second };
    const result = filterVisibility(spans, { ...filters, scopes });
    assert.deepStrictEqual([...result.matching], expected);
    assert.deepStrictEqual(durationOrderedIndexes(index, testPhase, filters), expected);
    assert.deepStrictEqual(durationOrderedIndexes(index, ROW_ORDERS.DURATION, filters), expected);
    assert.deepStrictEqual(durationOrderedIndexes(index, testPhase, {
      ...filters, minimumDurationNs: 31 * second,
    }), []);
    assert.deepStrictEqual(durationOrderedIndexes(index, testPhase, {
      ...filters, query: "missing case",
    }), []);
  }
}

function testOrderingDescendantsOfMatchingDurationGroups() {
  const filters = { testPhase: workers, minimumDurationNs: 29 * second };
  // The threshold measures workers, so their shorter tests remain eligible.
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, filters), [6, 14, 5]);
  assert.deepStrictEqual(durationOrderedIndexes(index, chunks, filters), [13, 3]);
  assert.deepStrictEqual(durationOrderedIndexes(index, prepare, filters), [4, 15]);
  assert.deepStrictEqual(durationOrderedIndexes(index, setup, filters), [2]);
  // Unqualified workers and their descendants must not enter through ancestors.
  assert.deepStrictEqual(durationOrderedIndexes(index, workers, filters), [1, 12]);
  assert.deepStrictEqual(
    durationOrderedIndexes(index, ROW_ORDERS.DURATION, filters),
    [1, 12],
  );
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, {
    ...filters, query: "target A", testSizes: new Set(["medium"]),
  }), [6, 5]);
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, {
    testPhase: chunks, minimumDurationNs: 27 * second,
  }), [14]);
  assert.deepStrictEqual(durationOrderedIndexes(index, tests, {
    ...filters, minimumDurationNs: 31 * second,
  }), []);
}

function testFlatOrderingPaginatesAndKeepsInlineMetadata() {
  const sorted = durationOrderedIndexes(index, tests);
  const first = flatOrderedRows(sorted, 2);
  assert.deepStrictEqual(spanIndexes(first), [6, 14]);
  assert(first.items.every((row) => row.depth === 0 && row.flat));
  assert.strictEqual(first.truncated, true);
  assert.deepStrictEqual(
    inlineDetailRows(first.items, 6).map((row) => row.kind),
    ["span", "detail", "span"],
  );
  const all = flatOrderedRows(sorted, 4);
  assert.deepStrictEqual(spanIndexes(all), sorted);
  assert.strictEqual(all.truncated, false);
}

function testOrderedChunkExpansionLeavesRoomOnAFullPage() {
  const sourceRoots = durationOrderedIndexes(index, chunks);
  const maximumRows = expandedGroupBudget(3, 3, index.children[13].length);
  const rows = flattenTraceRows({
    sourceSpans: spans, sourceChildren: index.children, sourceRoots,
    sourceExpanded: new Set([13]), maximumRows,
    sourceLimits: new Map([[13, initialGroupLimit(maximumRows, 3, 2)]]),
  });
  assert.deepStrictEqual(spanIndexes(rows), [13, 14, 15, 9, 3]);
  assert.strictEqual(rows.truncated, false);
}

for (const test of [
  testSharedDurationChoicesIncludeTestsAndInfrastructure,
  testOrderingUsesDisplayedDurationsAndStableTies,
  testFullWorkerDurationDoesNotSumOverlappingChildren,
  testFilteringAndOrderingAgreeOnReportedTestDuration,
  testOrderedPhasesUseTheirOwnersForComposedFilters,
  testOrderedGroupsCanBeFilteredByAnotherDurationChoice,
  testOrderedGroupsContainMatchingTests,
  testFlatOrderingExcludesAncestorsAndUnmatchedChildren,
  testGroupDurationCanMatchContainedTestNames,
  testOrderingDescendantsOfMatchingDurationGroups,
  testFlatOrderingPaginatesAndKeepsInlineMetadata,
  testOrderedChunkExpansionLeavesRoomOnAFullPage,
]) {
  test();
}
