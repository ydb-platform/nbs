"use strict";

require("../trace_report_js_testing.js");

function testDurationFormatting() {
  assert.strictEqual(formatDuration(0), "0 µs");
  assert.strictEqual(formatDuration(1_500_000), "1.5 ms");
  assert.strictEqual(formatDuration(2_500_000_000), "2.500 s");
  assert.strictEqual(formatDuration(65_000_000_000), "1m 5.0s");
  assert.strictEqual(formatDuration(3_665_000_000_000), "1h 1m 5s");

  const test = makeSpan({
    id: "test",
    name: "Suite::case",
    duration: 0,
    attributes: { "test.duration.reported_seconds": 2.5 },
  });
  assert.strictEqual(reportedTestDurationNs(test, "ya.test"), 2_500_000_000);
  assert.strictEqual(reportedTestDurationNs(test, "ya.chunk"), null);
  assert.strictEqual(
    reportedTestTimingNote(test, "ya.test"),
    "timestamp marker has no interval",
  );
  test[FIELDS.DURATION] = 2_500_000_000;
  assert.strictEqual(
    reportedTestTimingNote(test, "ya.test"),
    "timeline placement is inferred from chunk test order",
  );
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
    "ya.test",
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
    makeSpan({
      id: "ranked-test",
      parent: 1,
      name: "Suite::slow_test",
      scope: 5,
      attributes: { "ya.test.duration.rank": 1 },
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

  const topTenStages = filterVisibility(spans, {
    topTestsOnly: true,
    testPhase,
    scopes,
  });
  assert.strictEqual(topTenStages.matches, 2);
  assert.deepStrictEqual([...topTenStages.visible], [2, 1, 0, 3]);

  const slowTopTenStages = filterVisibility(spans, {
    topTestsOnly: true,
    minimumDurationNs: 10 * second,
    testPhase,
    scopes,
  });
  assert.strictEqual(slowTopTenStages.matches, 1);
  assert.deepStrictEqual([...slowTopTenStages.visible], [2, 1, 0]);
}

for (const test of [
  testDurationFormatting,
  testPhaseOptionsAreScopedAndHumanReadable,
  testPhaseDurationFilterMayMatchAParentTarget,
]) {
  test();
}
