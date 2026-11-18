"use strict";

require("../trace_report_js_testing.js");

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
    TIMELINE_MODES.GLOBAL,
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

for (const test of [
  testTimelineBarClassesAreSafeAndDeterministic,
  testTimelineModesAreValidatedAndLocalRootsAreExplicit,
  testGlobalModeKeepsAllRowsOnTheWorkflowTimeline,
  testWorkerNodeAndChunkResetTheLocalTimeline,
]) {
  test();
}
