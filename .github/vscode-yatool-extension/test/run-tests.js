const assert = require("assert");
const Module = require("module");
const path = require("path");

class Position {
  constructor(line, character) {
    this.line = line;
    this.character = character;
  }
}

class Range {
  constructor(start, end) {
    this.start = start;
    this.end = end;
  }

  contains(position) {
    if (position.line < this.start.line || position.line > this.end.line) {
      return false;
    }
    if (position.line === this.start.line && position.character < this.start.character) {
      return false;
    }
    if (position.line === this.end.line && position.character > this.end.character) {
      return false;
    }
    return true;
  }
}

class Uri {
  constructor(filePath) {
    this.path = filePath;
  }

  static file(filePath) {
    return new Uri(filePath);
  }

  static joinPath(base, ...parts) {
    return new Uri(path.posix.join(base.path, ...parts));
  }

  with(changes) {
    return new Uri(changes.path || this.path);
  }

  toString() {
    return `file://${this.path}`;
  }
}

const workspaceRoot = Uri.file("/repo");
const existingDirectories = new Set([
  "/repo/contrib/python/PySocks",
  "/repo/contrib/python/PySocks/py3",
]);
const existingFiles = new Set([
  "/repo/contrib/python/PySocks/ya.make",
]);
const vscodeMock = {
  FileType: { File: 1, Directory: 2 },
  Position,
  Range,
  Uri,
  workspace: {
    getWorkspaceFolder() {
      return { uri: workspaceRoot };
    },
    fs: {
      async stat(uri) {
        if (existingDirectories.has(uri.path)) {
          return { type: vscodeMock.FileType.Directory };
        }
        if (existingFiles.has(uri.path)) {
          return { type: vscodeMock.FileType.File };
        }
        throw new Error(`ENOENT: ${uri.path}`);
      },
    },
  },
};

const originalLoad = Module._load;
Module._load = function load(request, parent, isMain) {
  if (request === "vscode") {
    return vscodeMock;
  }
  return originalLoad.call(this, request, parent, isMain);
};

const { collectPathRefs, resolveRefLocation } = require("../pathResolver");
const { __testing: extensionTesting } = require("../extension");

function makeDocument(relativePath, text) {
  const uri = Uri.file(path.posix.join(workspaceRoot.path, relativePath));
  return {
    languageId: "yamake",
    uri,
    getText() {
      return text;
    },
    positionAt(offset) {
      let line = 0;
      let lineStart = 0;
      for (let i = 0; i < offset; i += 1) {
        if (text[i] === "\n") {
          line += 1;
          lineStart = i + 1;
        }
      }
      return new Position(line, offset - lineStart);
    },
  };
}

function refsFor(text, relativePath = "project/ya.make") {
  return collectPathRefs(makeDocument(relativePath, text));
}

function displays(refs) {
  return refs.map((ref) => ref.displayTarget);
}

function testPeerDirAddinclModifier() {
  const refs = refsFor(`
PEERDIR(
    ADDINCL contrib/libs/protobuf
    contrib/libs/protoc
)
`);
  assert.deepStrictEqual(displays(refs), ["contrib/libs/protobuf", "contrib/libs/protoc"]);
}

function testResourceDontParseOption() {
  const refs = refsFor("RESOURCE(DONT_PARSE ../SQLv1.g.in SQLv1.g.in)\n");
  assert.deepStrictEqual(displays(refs), ["../SQLv1.g.in"]);
}

function testPySrcsCythonDirective() {
  const refs = refsFor(`
PY_SRCS(
    CYTHON_DIRECTIVE
    language_level=3
    __res.pyx
)
`);
  assert.deepStrictEqual(displays(refs), ["__res.pyx"]);
}

function testGeneratedOutputsAreMarked() {
  const refs = refsFor(`
RUN_PYTHON3(
    tool.py
    OUT_NOAUTO generated.cc
    CWD \${BINDIR}
)

SRCS(
    generated.cc
    checked_in.cc
)
`);
  const generated = refs.find((ref) => ref.displayTarget === "generated.cc");
  const checkedIn = refs.find((ref) => ref.displayTarget === "checked_in.cc");
  assert.strictEqual(generated.generatedOutput, true);
  assert.strictEqual(checkedIn.generatedOutput, false);
}

function testGeneratedOutputsMatchResolvedPath() {
  const refs = refsFor(`
RUN_PYTHON3(
    tool.py
    OUT_NOAUTO llvm-symbolizer
)

RESOURCE(
    yt/yt/library/ytprof/bundle/llvm-symbolizer
    llvm-symbolizer
)
`, "yt/yt/library/ytprof/bundle/ya.make");
  assert.strictEqual(refs.length, 2);
  assert.strictEqual(refs[1].displayTarget, "yt/yt/library/ytprof/bundle/llvm-symbolizer");
  assert.strictEqual(refs[1].generatedOutput, true);
}

function testGoTestSourcesSkipValidation() {
  const refs = refsFor(`
GO_TEST_SRCS(missing_internal_test.go)
GO_XTEST_SRCS(missing_external_test.go)
`);
  assert.deepStrictEqual(displays(refs), ["missing_internal_test.go", "missing_external_test.go"]);
  assert.deepStrictEqual(refs.map((ref) => ref.skipValidation), [true, true]);
}

function testConditionalRefsAreMarked() {
  const refs = refsFor(`
IF (OS_EMSCRIPTEN)
    DEPENDS(contrib/restricted/emscripten/include)
ENDIF()
`);
  assert.strictEqual(refs.length, 1);
  assert.strictEqual(refs[0].inConditional, true);
}

function testGeneratedEnumSerializationIsHoverOnly() {
  const refs = refsFor("GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_config.pb.h)\n");
  assert.deepStrictEqual(refs, []);
}

function testIncAutodetectRequiresUppercaseMacroSpelling() {
  const cInc = makeDocument("contrib/python/numpy/py3/numpy/core/src/umath/funcs.inc", "    if (!one) {\n        return;\n    }\n");
  const yaInc = makeDocument("cloud/filestore/tests/recipes/service-kikimr.inc", "DEPENDS(cloud/filestore/apps/server)\n");
  assert.strictEqual(extensionTesting.looksLikeYaMakeFragment(cInc), false);
  assert.strictEqual(extensionTesting.looksLikeYaMakeFragment(yaInc), true);
}

async function testModuleDirectoryDoesNotUseAncestorYaMake() {
  const refs = refsFor("RECURSE(py3)\n", "contrib/python/PySocks/ya.make");
  assert.strictEqual(refs.length, 1);
  const location = await resolveRefLocation(refs[0], workspaceRoot);
  assert.strictEqual(location, undefined);
}

(async () => {
  for (const test of [
    testPeerDirAddinclModifier,
    testResourceDontParseOption,
    testPySrcsCythonDirective,
    testGeneratedOutputsAreMarked,
    testGeneratedOutputsMatchResolvedPath,
    testGoTestSourcesSkipValidation,
    testConditionalRefsAreMarked,
    testGeneratedEnumSerializationIsHoverOnly,
    testIncAutodetectRequiresUppercaseMacroSpelling,
    testModuleDirectoryDoesNotUseAncestorYaMake,
  ]) {
    await test();
  }

  console.log("All yatool extension tests passed.");
})();
