"use strict";

require("../trace_report_js_testing.js");

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

for (const test of [
  testMetadataDetailsAreInsertedLocallyAndToggleExclusively,
  testFailedYaArtifactPathsBecomeSafeLinks,
]) {
  test();
}
