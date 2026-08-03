"use strict";

require("../trace_report_js_testing.js");

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

for (const test of [
  testChunkDescendantsUseTheChunkAsTheirTimeline,
  testNearestChunkWinsAndLocalIntervalsAreClipped,
  testStageWithoutAUsableChunkKeepsTheGlobalTimeline,
]) {
  test();
}
