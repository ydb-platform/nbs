"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");

function loadTraceReportApi() {
  const templateDirectory = path.join(
    __dirname,
    "templates",
    "trace_report",
  );
  const wrapper = fs.readFileSync(
    path.join(__dirname, "templates", "trace_report.js"),
    "utf8",
  );
  const names = [...wrapper.matchAll(/{% include "trace_report\/(.+\.js)" %}/g)]
    .map((match) => match[1]);
  const source = names
    .map((name) => fs.readFileSync(path.join(templateDirectory, name), "utf8"))
    .join("\n");
  const loadedModule = { exports: {} };
  new Function("module", "exports", source)(
    loadedModule,
    loadedModule.exports,
  );
  return loadedModule.exports;
}

Object.assign(globalThis, loadTraceReportApi(), { assert });

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

function spanIndexes(rows) {
  return rows.items
    .filter((item) => item.kind === "span")
    .map((item) => item.index);
}

Object.assign(globalThis, { makeSpan, spanIndexes });

