"use strict";

const ID = 0;
const PARENT = 1;
const NAME = 2;
const START = 3;
const DURATION = 4;
const ATTRS = 5;
const EVENTS = 6;
const STATUS = 7;
const STATUS_MESSAGE = 8;
const RESOURCE = 9;
const SCOPE = 10;
const TRACE = 11;
const ORPHAN_PARENT = 12;
const PAGE_SIZE = 200;
const LOAD_SIZE_OPTIONS = Object.freeze([200, 1000, 5000]);
const INITIAL_ROW_BUDGET = PAGE_SIZE;
const NAME_COLUMN_STORAGE_KEY = "trace-report.name-column-width";
const MIN_NAME_COLUMN_WIDTH_PX = 240;
const MIN_OTHER_COLUMNS_WIDTH_PX = 400;
const NAME_COLUMN_KEYBOARD_STEP_PX = 24;
const MIN_TIMELINE_BAR_PERCENT = 0.15;
const TIMELINE_MODES = Object.freeze({
  GLOBAL: "global",
  LOCAL: "local",
});
const LOCAL_TIMELINE_ROOT_SCOPES = new Set([
  "ya.test.worker",
  "ya.test.node",
  "ya.chunk",
]);
const COLLAPSED_SCOPES = new Set([
  "ya.build",
  "ya.chunk",
  "ya.test.operations",
  "ya.test.node",
  "ya.test.worker",
]);
const TEST_SIZES = Object.freeze(["small", "medium", "large"]);
const TEST_PHASE_DEFINITIONS = Object.freeze([
  Object.freeze({
    scope: "ya.test.stage",
    attribute: "ya.test.stage.name",
    label: "Test stage",
    ownerScopes: Object.freeze(["ya.chunk"]),
  }),
  Object.freeze({
    scope: "ya.test.worker.phase",
    attribute: "ya.test.worker.phase",
    label: "Worker phase",
    ownerScopes: Object.freeze(["ya.test.worker", "ya.test.node"]),
  }),
]);
const TEST_PHASE_NAME_LABELS = Object.freeze({
  exec_cmd: "exec command",
  post_cmd: "post command",
  node_result: "node result",
});
const TEST_WORKER_PHASE_BAR_CLASSES = Object.freeze({
  setup: "bar-worker-setup",
  exec_cmd: "bar-worker-exec-cmd",
  post_cmd: "bar-worker-post-cmd",
  node_result: "bar-worker-node-result",
  finalize: "bar-worker-finalize",
});
const TEST_STAGE_BAR_CLASSES = Object.freeze({
  prepare_recipes: "bar-stage-prepare-recipes",
  wrapper_execution: "bar-stage-wrapper-execution",
  stop_recipes: "bar-stage-stop-recipes",
});
