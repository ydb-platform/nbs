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
const vscodeMock = {
  FileType: { Directory: 2 },
  Position,
  Range,
  Uri,
  workspace: {
    getWorkspaceFolder() {
      return { uri: workspaceRoot };
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

const { collectPathRefs } = require("../pathResolver");

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

for (const test of [
  testPeerDirAddinclModifier,
  testResourceDontParseOption,
  testPySrcsCythonDirective,
  testGeneratedOutputsAreMarked,
  testConditionalRefsAreMarked,
  testGeneratedEnumSerializationIsHoverOnly,
]) {
  test();
}

console.log("All yatool extension tests passed.");
