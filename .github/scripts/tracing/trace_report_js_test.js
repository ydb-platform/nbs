"use strict";

const assert = require("assert");
const {
  FIELDS,
  COLLAPSED_SCOPES,
  LOAD_SIZE_OPTIONS,
  INITIAL_ROW_BUDGET,
  NAME_COLUMN_STORAGE_KEY,
  TIMELINE_MODES,
  LOCAL_TIMELINE_ROOT_SCOPES,
  clampNameColumnWidth,
  readStoredNameColumnWidth,
  writeStoredNameColumnWidth,
  formatDuration,
  childCountLabel,
  directChildCountLabel,
  isCriticalPathTest,
  longestTestRank,
  timelineBarClass,
  timelineModeFromValue,
  timelineBarGeometry,
  buildHierarchy,
  defaultExpanded,
  matchingVisibility,
  filterVisibility,
  parseMinimumDurationNs,
  filterControlActivity,
  clearedFilterControlState,
  encodeTestPhaseSelection,
  parseTestPhaseSelection,
  testPhaseOptions,
  flattenTraceRows,
  loadSizeFromValue,
  nextRowLimit,
  groupLoadPlan,
  initialGroupLimit,
  inlineDetailRows,
  safeArtifactPath,
  artifactLinkForAttribute,
  linkableHttpUrl,
  nextSelectedSpan,
} = require("./templates/trace_report.js");

function makeSpan({
  id,
  parent = -1,
  name,
  scope = 0,
  attributes = {},
  statusMessage = "",
  status = 0,
  start = 0,
  duration = 1_000_000,
}) {
  const span = Array(13).fill("");
  span[FIELDS.ID] = id;
  span[FIELDS.PARENT] = parent;
  span[FIELDS.NAME] = name;
  span[FIELDS.START] = start;
  span[FIELDS.DURATION] = duration;
  span[FIELDS.ATTRS] = attributes;
  span[FIELDS.EVENTS] = [];
  span[FIELDS.STATUS] = status;
  span[FIELDS.STATUS_MESSAGE] = statusMessage;
  span[FIELDS.RESOURCE] = 0;
  span[FIELDS.SCOPE] = scope;
  span[FIELDS.TRACE] = 0;
  span[FIELDS.ORPHAN_PARENT] = "";
  return span;
}

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

function testTimelineBarClassesAreSafeAndDeterministic() {
  for (const [phase, expected] of Object.entries({
    setup: "bar-worker-setup",
    exec_cmd: "bar-worker-exec-cmd",
    post_cmd: "bar-worker-post-cmd",
    node_result: "bar-worker-node-result",
    finalize: "bar-worker-finalize",
  })) {
    assert.strictEqual(
      timelineBarClass("ya.test.worker.phase", {
        "ya.test.worker.phase": phase,
      }),
      expected,
    );
  }

  for (const [stage, expected] of Object.entries({
    prepare_recipes: "bar-stage-prepare-recipes",
    wrapper_execution: "bar-stage-wrapper-execution",
    stop_recipes: "bar-stage-stop-recipes",
  })) {
    assert.strictEqual(
      timelineBarClass("ya.test.stage", { "ya.test.stage.name": stage }),
      expected,
    );
  }

  assert.strictEqual(
    timelineBarClass("ya.test.stage", {
      "ya.test.stage.name": "setup_environment",
    }),
    "bar-stage-other",
  );
  assert.strictEqual(
    timelineBarClass("ya.test.worker.phase", {
      "ya.test.worker.phase": 'setup bad-class" onclick="alert(1)',
    }),
    "bar-default",
  );
  assert.strictEqual(
    timelineBarClass("ya.test.worker.phase", {
      "ya.test.worker.phase": "__proto__",
    }),
    "bar-default",
  );
  assert.strictEqual(
    timelineBarClass("ya.test", { "ya.test.worker.phase": "setup" }),
    "bar-default",
  );
  assert.strictEqual(timelineBarClass("ya.test.stage", null), "bar-stage-other");
}

function testTimelineModesAreValidatedAndLocalRootsAreExplicit() {
  assert.strictEqual(timelineModeFromValue("global"), TIMELINE_MODES.GLOBAL);
  assert.strictEqual(timelineModeFromValue("local"), TIMELINE_MODES.LOCAL);
  assert.strictEqual(
    timelineModeFromValue("unexpected"),
    TIMELINE_MODES.LOCAL,
  );
  assert.deepStrictEqual(
    [...LOCAL_TIMELINE_ROOT_SCOPES],
    ["ya.test.worker", "ya.test.node", "ya.chunk"],
  );
}

function testGlobalModeKeepsAllRowsOnTheWorkflowTimeline() {
  const second = 1_000_000_000;
  const scopes = [
    "workflow",
    "ya.test.node",
    "ya.test.worker.phase",
    "ya.chunk",
    "ya.test.stage",
  ];
  const spans = [
    makeSpan({
      id: "workflow",
      name: "workflow",
      scope: 0,
      duration: 1000 * second,
    }),
    makeSpan({
      id: "node",
      parent: 0,
      name: "test result aggregation",
      scope: 1,
      start: 100 * second,
      duration: 400 * second,
    }),
    makeSpan({
      id: "phase",
      parent: 1,
      name: "worker setup",
      scope: 2,
      start: 120 * second,
      duration: 20 * second,
    }),
    makeSpan({
      id: "chunk",
      parent: 1,
      name: "chunk",
      scope: 3,
      start: 200 * second,
      duration: 100 * second,
    }),
    makeSpan({
      id: "stage",
      parent: 3,
      name: "prepare recipes",
      scope: 4,
      start: 210 * second,
      duration: 10 * second,
    }),
  ];

  for (const [index, expected] of [
    [1, { left: 10, width: 40, relativeTo: -1 }],
    [2, { left: 12, width: 2, relativeTo: -1 }],
    [3, { left: 20, width: 10, relativeTo: -1 }],
    [4, { left: 21, width: 1, relativeTo: -1 }],
  ]) {
    assert.deepStrictEqual(
      timelineBarGeometry(
        index,
        spans,
        scopes,
        spans[0][FIELDS.DURATION],
        TIMELINE_MODES.GLOBAL,
      ),
      expected,
    );
  }
}

function testWorkerNodeAndChunkResetTheLocalTimeline() {
  const second = 1_000_000_000;
  const scopes = [
    "workflow",
    "ya.test.node",
    "ya.test.worker.phase",
    "ya.test.worker",
    "ya.chunk",
    "ya.test.stage",
    "group",
  ];
  const spans = [
    makeSpan({
      id: "workflow",
      name: "workflow",
      scope: 0,
      duration: 1000 * second,
    }),
    makeSpan({
      id: "node",
      parent: 0,
      name: "test result aggregation",
      scope: 1,
      start: 100 * second,
      duration: 400 * second,
    }),
    makeSpan({
      id: "node-phase",
      parent: 1,
      name: "node setup",
      scope: 2,
      start: 120 * second,
      duration: 20 * second,
    }),
    makeSpan({
      id: "worker",
      parent: 1,
      name: "test worker",
      scope: 3,
      start: 150 * second,
      duration: 300 * second,
    }),
    makeSpan({
      id: "worker-phase",
      parent: 3,
      name: "worker setup",
      scope: 2,
      start: 165 * second,
      duration: 15 * second,
    }),
    makeSpan({
      id: "chunk",
      parent: 3,
      name: "chunk",
      scope: 4,
      start: 200 * second,
      duration: 100 * second,
    }),
    makeSpan({
      id: "group",
      parent: 5,
      name: "preparation",
      scope: 6,
      start: 200 * second,
      duration: 100 * second,
    }),
    makeSpan({
      id: "stage",
      parent: 6,
      name: "prepare recipes",
      scope: 5,
      start: 210 * second,
      duration: 10 * second,
    }),
  ];
  const traceDuration = spans[0][FIELDS.DURATION];

  for (const index of [1, 3, 5]) {
    assert.deepStrictEqual(
      timelineBarGeometry(
        index,
        spans,
        scopes,
        traceDuration,
        TIMELINE_MODES.LOCAL,
      ),
      { left: 0, width: 100, relativeTo: index },
    );
  }
  assert.deepStrictEqual(
    timelineBarGeometry(
      2,
      spans,
      scopes,
      traceDuration,
      TIMELINE_MODES.LOCAL,
    ),
    { left: 5, width: 5, relativeTo: 1 },
  );
  assert.deepStrictEqual(
    timelineBarGeometry(
      4,
      spans,
      scopes,
      traceDuration,
      TIMELINE_MODES.LOCAL,
    ),
    { left: 5, width: 5, relativeTo: 3 },
  );
  assert.deepStrictEqual(
    timelineBarGeometry(
      7,
      spans,
      scopes,
      traceDuration,
      TIMELINE_MODES.LOCAL,
    ),
    { left: 10, width: 10, relativeTo: 5 },
  );
}

function testChunkDescendantsUseTheChunkAsTheirTimeline() {
  const second = 1_000_000_000;
  const scopes = [
    "workflow",
    "ya.chunk",
    "group",
    "ya.test.stage",
    "ya.test",
    "ya.test.worker.phase",
  ];
  const chunkStart = 60 * 60 * second;
  const chunkDuration = 258.2 * second;
  const spans = [
    makeSpan({
      id: "workflow",
      name: "three hour workflow",
      scope: 0,
      duration: 3 * 60 * 60 * second,
    }),
    makeSpan({
      id: "chunk",
      parent: 0,
      name: "cloud/blockstore/tests/loadtest/local-endpoints [py3test chunk 2/8]",
      scope: 1,
      start: chunkStart,
      duration: chunkDuration,
    }),
    makeSpan({
      id: "group",
      parent: 1,
      name: "preparation",
      scope: 2,
      start: chunkStart,
      duration: chunkDuration,
    }),
    makeSpan({
      id: "stage",
      parent: 2,
      name: "prepare recipes",
      scope: 3,
      start: chunkStart + second,
      duration: 15 * second,
    }),
    makeSpan({
      id: "direct-stage",
      parent: 1,
      name: "wrapper execution",
      scope: 3,
      start: chunkStart + 16 * second,
      duration: second,
    }),
    makeSpan({
      id: "test",
      parent: 1,
      name: "test case",
      scope: 4,
      start: chunkStart + second,
      duration: 15 * second,
    }),
    makeSpan({
      id: "worker-phase",
      parent: 0,
      name: "worker setup",
      scope: 5,
      start: chunkStart + second,
      duration: 15 * second,
    }),
  ];
  const traceDuration = spans[0][FIELDS.DURATION];

  assert.deepStrictEqual(
    timelineBarGeometry(1, spans, scopes, traceDuration),
    { left: 0, width: 100, relativeTo: 1 },
  );
  const stage = timelineBarGeometry(3, spans, scopes, traceDuration);
  assert.strictEqual(stage.relativeTo, 1);
  assert(Math.abs(stage.left - (100 * 1) / 258.2) < 1e-9);
  assert(Math.abs(stage.width - (100 * 15) / 258.2) < 1e-9);
  const directStage = timelineBarGeometry(4, spans, scopes, traceDuration);
  assert.strictEqual(directStage.relativeTo, 1);
  assert(Math.abs(directStage.left - (100 * 16) / 258.2) < 1e-9);
  assert(Math.abs(directStage.width - 100 / 258.2) < 1e-9);

  const test = timelineBarGeometry(5, spans, scopes, traceDuration);
  assert.strictEqual(test.relativeTo, 1);
  assert(Math.abs(test.left - (100 * 1) / 258.2) < 1e-9);
  assert(Math.abs(test.width - (100 * 15) / 258.2) < 1e-9);

  const workerPhase = timelineBarGeometry(6, spans, scopes, traceDuration);
  assert.strictEqual(workerPhase.relativeTo, -1);
  assert(Math.abs(workerPhase.left - (100 * 3601) / 10800) < 1e-9);
  assert.strictEqual(workerPhase.width, 0.15);
}

function testNearestChunkWinsAndLocalIntervalsAreClipped() {
  const second = 1_000_000_000;
  const scopes = ["workflow", "ya.chunk", "ya.test.stage", "ya.test"];
  const spans = [
    makeSpan({
      id: "workflow",
      name: "workflow",
      scope: 0,
      duration: 100 * second,
    }),
    makeSpan({
      id: "outer-chunk",
      parent: 0,
      name: "outer chunk",
      scope: 1,
      start: 10 * second,
      duration: 80 * second,
    }),
    makeSpan({
      id: "inner-chunk",
      parent: 1,
      name: "inner chunk",
      scope: 1,
      start: 20 * second,
      duration: 20 * second,
    }),
    makeSpan({
      id: "inner-test",
      parent: 2,
      name: "inner test",
      scope: 3,
      start: 25 * second,
      duration: 5 * second,
    }),
    makeSpan({
      id: "overrunning-stage",
      parent: 2,
      name: "overrunning stage",
      scope: 2,
      start: 35 * second,
      duration: 10 * second,
    }),
    makeSpan({
      id: "outside-stage",
      parent: 2,
      name: "outside stage",
      scope: 2,
      start: 45 * second,
      duration: second,
    }),
    makeSpan({
      id: "instant-test",
      parent: 2,
      name: "instant test",
      scope: 3,
      start: 25 * second,
      duration: 0,
    }),
    makeSpan({
      id: "tiny-tail-stage",
      parent: 2,
      name: "tiny stage at the end",
      scope: 2,
      start: 40 * second - 1_000_000,
      duration: 1_000_000,
    }),
    makeSpan({
      id: "instant-at-end",
      parent: 2,
      name: "instant at the end",
      scope: 3,
      start: 40 * second,
      duration: 0,
    }),
  ];

  assert.deepStrictEqual(timelineBarGeometry(3, spans, scopes, 100 * second), {
    left: 25,
    width: 25,
    relativeTo: 2,
  });
  assert.deepStrictEqual(timelineBarGeometry(4, spans, scopes, 100 * second), {
    left: 75,
    width: 25,
    relativeTo: 2,
  });
  assert.deepStrictEqual(timelineBarGeometry(5, spans, scopes, 100 * second), {
    left: 100,
    width: 0,
    relativeTo: 2,
  });
  assert.deepStrictEqual(timelineBarGeometry(6, spans, scopes, 100 * second), {
    left: 25,
    width: 0.15,
    relativeTo: 2,
  });
  assert.deepStrictEqual(timelineBarGeometry(7, spans, scopes, 100 * second), {
    left: 99.85,
    width: 0.15,
    relativeTo: 2,
  });
  assert.deepStrictEqual(timelineBarGeometry(8, spans, scopes, 100 * second), {
    left: 99.85,
    width: 0.15,
    relativeTo: 2,
  });
}

function testStageWithoutAUsableChunkKeepsTheGlobalTimeline() {
  const second = 1_000_000_000;
  const scopes = ["workflow", "ya.chunk", "ya.test.stage"];
  const spans = [
    makeSpan({
      id: "workflow",
      name: "workflow",
      scope: 0,
      duration: 100 * second,
    }),
    makeSpan({
      id: "zero-chunk",
      parent: 0,
      name: "zero length chunk",
      scope: 1,
      start: 10 * second,
      duration: 0,
    }),
    makeSpan({
      id: "stage",
      parent: 1,
      name: "stage",
      scope: 2,
      start: 10 * second,
      duration: second,
    }),
    makeSpan({
      id: "orphan-stage",
      name: "orphan stage",
      scope: 2,
      start: 20 * second,
      duration: 2 * second,
    }),
  ];

  assert.deepStrictEqual(timelineBarGeometry(1, spans, scopes, 100 * second), {
    left: 10,
    width: 0.15,
    relativeTo: -1,
  });
  assert.deepStrictEqual(timelineBarGeometry(2, spans, scopes, 100 * second), {
    left: 10,
    width: 1,
    relativeTo: -1,
  });
  assert.deepStrictEqual(timelineBarGeometry(3, spans, scopes, 100 * second), {
    left: 20,
    width: 2,
    relativeTo: -1,
  });
}

function testMetadataDetailsAreInsertedLocallyAndToggleExclusively() {
  const rows = [
    { kind: "span", index: 10, depth: 1 },
    { kind: "span", index: 11, depth: 2 },
    { kind: "more", index: 10, depth: 2 },
    { kind: "span", index: 12, depth: 1 },
  ];

  assert.deepStrictEqual(inlineDetailRows(rows, 11), [
    rows[0],
    rows[1],
    { kind: "detail", index: 11, depth: 2 },
    rows[2],
    rows[3],
  ]);
  assert.deepStrictEqual(inlineDetailRows(rows, 99), rows);
  assert.strictEqual(
    inlineDetailRows(rows, 11).filter((row) => row.kind === "detail").length,
    1,
  );

  let selected = nextSelectedSpan(null, 11);
  assert.strictEqual(selected, 11);
  selected = nextSelectedSpan(selected, 12);
  assert.strictEqual(selected, 12);
  selected = nextSelectedSpan(selected, 12);
  assert.strictEqual(selected, null);

  assert.strictEqual(
    linkableHttpUrl("https://example.test/log?part=1"),
    "https://example.test/log?part=1",
  );
  assert.strictEqual(
    linkableHttpUrl("http://example.test/log"),
    "http://example.test/log",
  );
  assert.strictEqual(linkableHttpUrl("javascript:alert(1)"), null);
  assert.strictEqual(linkableHttpUrl("data:text/html,unsafe"), null);
  assert.strictEqual(linkableHttpUrl("plain metadata"), null);
}

function testFailedYaArtifactPathsBecomeSafeLinks() {
  const links = {
    testLog: "https://logs.example.test/run/logs/1/",
    testData: "https://logs.example.test/run/test_data/1/workspace/",
  };
  const failedTest = makeSpan({
    id: "failed-test",
    name: "failed test",
    status: 2,
  });
  const failedChunk = makeSpan({
    id: "failed-chunk",
    name: "failed chunk",
    status: 2,
  });
  const passingTest = makeSpan({
    id: "passing-test",
    name: "passing test",
    status: 1,
  });

  assert.strictEqual(
    safeArtifactPath("suite/test-results/py3 test/stderr.log"),
    "suite/test-results/py3%20test/stderr.log",
  );
  for (const unsafe of [
    "",
    "/absolute/path",
    "suite/../secret",
    "suite/./log",
    "suite//log",
    "suite\\log",
    "suite/log?download=1",
    "suite/log#tail",
    "suite/log\u0000tail",
  ]) {
    assert.strictEqual(safeArtifactPath(unsafe), null, unsafe);
  }

  assert.strictEqual(
    artifactLinkForAttribute(
      failedTest,
      "ya.test.log.recipe_stderr.path",
      "suite/test-results/py3 test/recipe.err",
      links,
    ),
    "https://logs.example.test/run/logs/1/suite/test-results/py3%20test/recipe.err",
  );
  assert.strictEqual(
    artifactLinkForAttribute(
      failedTest,
      "ya.test.logs_directory.path",
      "suite/test-results/py3test/testing_out_stuff",
      links,
    ),
    "https://logs.example.test/run/test_data/1/workspace/suite/test-results/py3test/testing_out_stuff/index.html",
  );
  assert.strictEqual(
    artifactLinkForAttribute(
      failedChunk,
      "ya.chunk.log.log.path",
      "suite/test-results/py3test/run_test.log",
      links,
    ),
    "https://logs.example.test/run/logs/1/suite/test-results/py3test/run_test.log",
  );
  assert.strictEqual(
    artifactLinkForAttribute(
      failedChunk,
      "ya.chunk.logs_directory.path",
      "suite/test-results/py3test/testing_out_stuff",
      links,
    ),
    "https://logs.example.test/run/test_data/1/workspace/suite/test-results/py3test/testing_out_stuff/index.html",
  );

  assert.strictEqual(
    artifactLinkForAttribute(
      passingTest,
      "ya.test.log.stdout.path",
      "suite/stdout",
      links,
    ),
    null,
  );
  assert.strictEqual(
    artifactLinkForAttribute(
      failedTest,
      "ya.test.metric.output.path",
      "suite/output",
      links,
    ),
    null,
  );
  assert.strictEqual(
    artifactLinkForAttribute(
      failedTest,
      "ya.test.log.stdout.path",
      "suite/output",
      { testLog: "javascript:alert(1)", testData: links.testData },
    ),
    null,
  );
  assert.strictEqual(
    artifactLinkForAttribute(
      failedTest,
      "ya.test.log.stdout.path",
      "suite/output",
      { testLog: "https://logs.example.test/root?unsafe=1" },
    ),
    null,
  );
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

function testPhaseOptionsAreScopedAndHumanReadable() {
  const scopes = ["workflow", "ya.test.stage", "ya.test.worker.phase"];
  const spans = [
    makeSpan({ id: "root", name: "root" }),
    makeSpan({
      id: "prepare",
      parent: 0,
      name: "prepare_recipes",
      scope: 1,
      attributes: { "ya.test.stage.name": "prepare_recipes" },
    }),
    makeSpan({
      id: "prepare-duplicate",
      parent: 0,
      name: "prepare_recipes",
      scope: 1,
      attributes: { "ya.test.stage.name": "prepare_recipes" },
    }),
    makeSpan({
      id: "stage-setup",
      parent: 0,
      name: "setup",
      scope: 1,
      attributes: { "ya.test.stage.name": "setup" },
    }),
    makeSpan({
      id: "worker-setup",
      parent: 0,
      name: "setup",
      scope: 2,
      attributes: { "ya.test.worker.phase": "setup" },
    }),
    makeSpan({
      id: "worker-exec",
      parent: 0,
      name: "exec_cmd",
      scope: 2,
      attributes: { "ya.test.worker.phase": "exec_cmd" },
    }),
  ];

  const options = testPhaseOptions(spans, scopes);
  assert.deepStrictEqual(
    options.map(({ label, scope, name }) => ({ label, scope, name })),
    [
      {
        label: "Any test stage",
        scope: "ya.test.stage",
        name: null,
      },
      {
        label: "Any worker phase",
        scope: "ya.test.worker.phase",
        name: null,
      },
      {
        label: "Test stage: prepare recipes",
        scope: "ya.test.stage",
        name: "prepare_recipes",
      },
      {
        label: "Test stage: setup",
        scope: "ya.test.stage",
        name: "setup",
      },
      {
        label: "Worker phase: exec command",
        scope: "ya.test.worker.phase",
        name: "exec_cmd",
      },
      {
        label: "Worker phase: setup",
        scope: "ya.test.worker.phase",
        name: "setup",
      },
    ],
  );
  assert.deepStrictEqual(parseTestPhaseSelection(options[0].value), {
    scope: "ya.test.stage",
    name: null,
  });
  assert.deepStrictEqual(parseTestPhaseSelection(options[3].value), {
    scope: "ya.test.stage",
    name: "setup",
  });
  assert.deepStrictEqual(parseTestPhaseSelection(options[5].value), {
    scope: "ya.test.worker.phase",
    name: "setup",
  });
  assert.notStrictEqual(options[3].value, options[5].value);
  assert.strictEqual(parseTestPhaseSelection(""), null);
  assert.strictEqual(parseTestPhaseSelection("not JSON"), null);
  assert.strictEqual(
    parseTestPhaseSelection(JSON.stringify(["unknown.scope", "setup"])),
    null,
  );
}

function testPhaseDurationFilterMayMatchAParentTarget() {
  const second = 1_000_000_000;
  const scopes = [
    "workflow",
    "ya.test.stage",
    "ya.test.worker.phase",
    "ya.chunk",
    "ya.test.worker",
  ];
  const spans = [
    makeSpan({ id: "root", name: "root", status: 2 }),
    makeSpan({
      id: "target",
      parent: 0,
      name: "cloud/blockstore/tests/loadtest/local-endpoints [py3test]",
      scope: 3,
      status: 2,
      attributes: {
        "test.size": "medium",
        "ya.test.duration.rank": 1,
      },
    }),
    makeSpan({
      id: "slow-prepare",
      parent: 1,
      name: "prepare_recipes",
      scope: 1,
      duration: 15 * second,
      attributes: { "ya.test.stage.name": "prepare_recipes" },
    }),
    makeSpan({
      id: "short-prepare",
      parent: 1,
      name: "prepare_recipes",
      scope: 1,
      duration: 5 * second,
      attributes: { "ya.test.stage.name": "prepare_recipes" },
    }),
    makeSpan({
      id: "long-wrapper",
      parent: 1,
      name: "wrapper_execution",
      scope: 1,
      duration: 60 * second,
      attributes: { "ya.test.stage.name": "wrapper_execution" },
    }),
    makeSpan({
      id: "worker-exec",
      parent: 1,
      name: "exec command",
      scope: 2,
      duration: 30 * second,
      attributes: { "ya.test.worker.phase": "exec_cmd" },
    }),
    makeSpan({
      id: "other-target",
      parent: 0,
      name: "another target",
      scope: 3,
    }),
    makeSpan({
      id: "other-prepare",
      parent: 6,
      name: "prepare_recipes",
      scope: 1,
      duration: 45 * second,
      attributes: { "ya.test.stage.name": "prepare_recipes" },
    }),
    makeSpan({
      id: "passing-worker",
      parent: 0,
      name: "passing worker",
      scope: 4,
    }),
    makeSpan({
      id: "passing-worker-setup",
      parent: 8,
      name: "worker phase: setup",
      scope: 2,
      attributes: { "ya.test.worker.phase": "setup" },
    }),
    makeSpan({
      id: "failed-worker",
      parent: 0,
      name: "failed worker",
      scope: 4,
      status: 2,
    }),
    makeSpan({
      id: "failed-worker-setup",
      parent: 10,
      name: "worker phase: setup",
      scope: 2,
      attributes: { "ya.test.worker.phase": "setup" },
    }),
  ];
  const testPhase = encodeTestPhaseSelection(
    "ya.test.stage",
    "prepare_recipes",
  );

  const byParent = filterVisibility(spans, {
    query: "cloud/blockstore/tests/loadtest/local-endpoints",
    minimumDurationNs: 10 * second,
    testPhase,
    scopes,
  });
  assert.strictEqual(byParent.matches, 1);
  assert.deepStrictEqual([...byParent.visible], [2, 1, 0]);

  const withContextualPredicates = filterVisibility(spans, {
    query: "cloud/blockstore/tests/loadtest/local-endpoints",
    failedOnly: true,
    testSizes: new Set(["medium"]),
    minimumDurationNs: 10 * second,
    testPhase,
    scopes,
  });
  assert.strictEqual(withContextualPredicates.matches, 1);
  assert.deepStrictEqual([...withContextualPredicates.visible], [2, 1, 0]);

  const failedStages = filterVisibility(spans, {
    failedOnly: true,
    testPhase,
    scopes,
  });
  assert.strictEqual(failedStages.matches, 2);
  assert.deepStrictEqual([...failedStages.visible], [2, 1, 0, 3]);
  assert.strictEqual(failedStages.visible.has(7), false);

  const bySelectedSpan = filterVisibility(spans, {
    query: "ya.test.stage.name=prepare_recipes",
    minimumDurationNs: 10 * second,
    testPhase,
    scopes,
  });
  assert.strictEqual(bySelectedSpan.matches, 2);
  assert.deepStrictEqual([...bySelectedSpan.visible], [2, 1, 0, 7, 6]);

  const anyTestStage = filterVisibility(spans, {
    minimumDurationNs: 10 * second,
    testPhase: encodeTestPhaseSelection("ya.test.stage"),
    scopes,
  });
  assert.strictEqual(anyTestStage.matches, 3);
  assert.strictEqual(anyTestStage.visible.has(5), false);

  const workerExec = filterVisibility(spans, {
    query: "cloud/blockstore/tests/loadtest/local-endpoints",
    minimumDurationNs: 20 * second,
    testPhase: encodeTestPhaseSelection(
      "ya.test.worker.phase",
      "exec_cmd",
    ),
    scopes,
  });
  assert.strictEqual(workerExec.matches, 1);
  assert.deepStrictEqual([...workerExec.visible], [5, 1, 0]);

  const failedWorkerPhases = filterVisibility(spans, {
    failedOnly: true,
    testPhase: encodeTestPhaseSelection(
      "ya.test.worker.phase",
      "setup",
    ),
    scopes,
  });
  assert.strictEqual(failedWorkerPhases.matches, 1);
  assert.deepStrictEqual([...failedWorkerPhases.visible], [11, 10, 0]);
  assert.strictEqual(failedWorkerPhases.visible.has(9), false);

  const strictTopTen = filterVisibility(spans, {
    topTestsOnly: true,
    testPhase,
    scopes,
  });
  assert.strictEqual(strictTopTen.matches, 0);
  assert.deepStrictEqual([...strictTopTen.visible], []);
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
  testNameColumnWidthIsClampedToAUsableLayout,
  testNameColumnWidthPersistenceHasSafeFallbacks,
  testDurationFormatting,
  testPhaseOptionsAreScopedAndHumanReadable,
  testPhaseDurationFilterMayMatchAParentTarget,
  testCustomMinimumDurationUsesSeconds,
  testFilterClearStateUsesRawValuesAndPreservesDisplayControls,
  testTimelineBarClassesAreSafeAndDeterministic,
  testTimelineModesAreValidatedAndLocalRootsAreExplicit,
  testGlobalModeKeepsAllRowsOnTheWorkflowTimeline,
  testWorkerNodeAndChunkResetTheLocalTimeline,
  testChunkDescendantsUseTheChunkAsTheirTimeline,
  testNearestChunkWinsAndLocalIntervalsAreClipped,
  testStageWithoutAUsableChunkKeepsTheGlobalTimeline,
  testMetadataDetailsAreInsertedLocallyAndToggleExclusively,
  testFailedYaArtifactPathsBecomeSafeLinks,
  testFailureFilterKeepsAncestorsButNotAggregateDescendants,
  testTextSearchDescendantsDoNotBypassComposedFilters,
  testFiltersComposeByIntersection,
  testTopTenTestsUseProducerRank,
  testSizeFiltersAreInclusiveAndIgnoreCase,
  testLoadSizesAreProgressiveIncrements,
  testChildCountsAndCriticalPathMarker,
  testChunkChildCountExcludesPreparationStages,
  testCollapsedGroupsAndExpansion,
  testSearchIncludesAttributesAndAncestors,
  testSearchKeepsChildrenOfMatchingTestOperations,
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
