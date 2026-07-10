const path = require("path");
const vscode = require("vscode");

const { MACRO_DOCS } = require("./macroMetadata");
const { parseMakelist } = require("./makelistParser");
const {
  collectPathRefs,
  findExistingDirectoryUri,
  findExistingFileUri,
  findModuleReferenceTarget,
  findOwnerYaMake,
  findPathRefAtPosition,
  isDirectory,
  isModuleDirectoryKind,
  isModuleReferenceKind,
  isPlainDirectoryKind,
  isSourceLikeFileKind,
  resolveLinkTarget,
  resolveRefLocation,
} = require("./pathResolver");
const {
  clearResourceMappingCache,
  findResourceMapping,
  findResourceRefAtPosition,
  formatResourceHover,
  isResourceMappingDocument,
} = require("./resourceMappings");

let diagnostics;

function activate(context) {
  diagnostics = vscode.languages.createDiagnosticCollection("yatool");
  context.subscriptions.push(diagnostics);

  const selector = [
    { language: "yamake" },
    { pattern: "**/ya.make" },
    { pattern: "**/*.make.inc" },
    { pattern: "**/*.ya.make.inc" },
  ];

  context.subscriptions.push(
    vscode.languages.registerDocumentLinkProvider(selector, {
      provideDocumentLinks(document) {
        const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
        return Promise.all(
          collectPathRefs(document)
            .filter((ref) => ref.target)
            .map(async (ref) => {
              const target = await resolveLinkTarget(ref, workspaceFolder && workspaceFolder.uri);
              const link = new vscode.DocumentLink(ref.range, target);
              link.tooltip = `Open ${ref.displayTarget}`;
              return link;
            }),
        );
      },
    }),
  );

  context.subscriptions.push(
    vscode.languages.registerDefinitionProvider(selector, {
      async provideDefinition(document, position) {
        const pathRef = findPathRefAtPosition(document, position);
        if (pathRef) {
          const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
          return resolveRefLocation(pathRef, workspaceFolder && workspaceFolder.uri);
        }

        const ref = findResourceRefAtPosition(document, position);
        if (!ref) {
          return undefined;
        }
        const mapping = await findResourceMapping(document, ref.value);
        return mapping ? mapping.location : undefined;
      },
    }),
  );

  context.subscriptions.push(
    vscode.languages.registerHoverProvider(selector, {
      async provideHover(document, position) {
        const macro = findMacroAtPosition(document, position);
        if (macro) {
          return new vscode.Hover(formatMacroHover(macro));
        }

        const resource = findResourceRefAtPosition(document, position);
        if (resource) {
          const mapping = await findResourceMapping(document, resource.value);
          if (mapping) {
            return new vscode.Hover(formatResourceHover(resource.value, mapping));
          }
        }

        return undefined;
      },
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("yatool.refreshDiagnostics", () => {
      for (const document of vscode.workspace.textDocuments) {
        if (isYaMakeDocument(document)) {
          validateDocument(document);
        }
      }
    }),
  );

  context.subscriptions.push(
    vscode.workspace.onDidOpenTextDocument((document) => {
      detectYaMakeIncDocument(document).then(validateIfYaMakeDocument);
    }),
    vscode.workspace.onDidChangeTextDocument((event) => {
      if (isYaMakeDocument(event.document)) {
        validateDocument(event.document);
      }
    }),
    vscode.workspace.onDidCloseTextDocument((document) => diagnostics.delete(document.uri)),
    vscode.workspace.onDidSaveTextDocument((document) => {
      if (isResourceMappingDocument(document)) {
        clearResourceMappingCache();
      }
    }),
  );

  for (const document of vscode.workspace.textDocuments) {
    detectYaMakeIncDocument(document).then(validateIfYaMakeDocument);
  }
}

function deactivate() {}

function isYaMakeDocument(document) {
  const base = path.posix.basename(document.uri.path);
  return document.languageId === "yamake" || base === "ya.make" || base.endsWith(".make.inc") || base.endsWith(".ya.make.inc");
}

async function detectYaMakeIncDocument(document) {
  if (document.languageId === "yamake" || !isBareIncDocument(document) || !looksLikeYaMakeFragment(document)) {
    return document;
  }
  return vscode.languages.setTextDocumentLanguage(document, "yamake");
}

function validateIfYaMakeDocument(document) {
  if (isYaMakeDocument(document)) {
    validateDocument(document);
  }
}

function isBareIncDocument(document) {
  const base = path.posix.basename(document.uri.path);
  return base.endsWith(".inc") && !base.endsWith(".make.inc") && !base.endsWith(".ya.make.inc");
}

function looksLikeYaMakeFragment(document) {
  const text = document.getText();
  const parsed = parseMakelist(text);

  for (const call of parsed.calls) {
    if (MACRO_DOCS[call.name] && isAtLineStartIgnoringWhitespace(text, call.nameStart)) {
      return true;
    }
  }

  return false;
}

function isAtLineStartIgnoringWhitespace(text, offset) {
  let i = offset - 1;
  while (i >= 0 && text[i] !== "\n") {
    if (text[i] !== " " && text[i] !== "\t" && text[i] !== "\r") {
      return false;
    }
    i -= 1;
  }
  return true;
}

async function validateDocument(document) {
  const parsed = parseMakelist(document.getText());
  const refs = collectPathRefs(document, parsed);
  const foundDiagnostics = [];

  for (const error of parsed.errors) {
    foundDiagnostics.push(
      new vscode.Diagnostic(
        new vscode.Range(document.positionAt(error.start), document.positionAt(error.end)),
        error.message,
        vscode.DiagnosticSeverity.Error,
      ),
    );
  }

  for (const ref of refs) {
    if (ref.inConditional || ref.generatedOutput) {
      continue;
    }

    if (ref.unresolvedReason) {
      foundDiagnostics.push(
        new vscode.Diagnostic(ref.range, ref.unresolvedReason, vscode.DiagnosticSeverity.Information),
      );
      continue;
    }

    if (!ref.target) {
      continue;
    }

    if (isSourceLikeFileKind(ref.kind)) {
      const target = await findExistingFileUri(ref.candidateTargets || [ref.target]);
      if (!target) {
        foundDiagnostics.push(
          new vscode.Diagnostic(ref.range, `File does not exist: ${ref.displayTarget}`, vscode.DiagnosticSeverity.Error),
        );
      }
      continue;
    }

    if (isPlainDirectoryKind(ref.kind)) {
      const target = await findExistingDirectoryUri(ref.candidateTargets || [ref.target]);
      if (!target) {
        foundDiagnostics.push(
          new vscode.Diagnostic(ref.range, `Directory does not exist: ${ref.displayTarget}`, vscode.DiagnosticSeverity.Error),
        );
      }
      continue;
    }

    if (isModuleReferenceKind(ref.kind)) {
      const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
      const owner = await findModuleReferenceTarget(ref.target, workspaceFolder && workspaceFolder.uri);
      if (!owner) {
        foundDiagnostics.push(
          new vscode.Diagnostic(ref.range, `${ref.macro} path has no owning ya.make: ${ref.displayTarget}`, vscode.DiagnosticSeverity.Warning),
        );
      }
      continue;
    }

    try {
      const stat = await vscode.workspace.fs.stat(ref.target);
      if (ref.kind === "file" && isDirectory(stat)) {
        foundDiagnostics.push(
          new vscode.Diagnostic(ref.range, "INCLUDE path resolves to a directory, expected a file.", vscode.DiagnosticSeverity.Error),
        );
      } else if (isModuleDirectoryKind(ref.kind) && !isDirectory(stat)) {
        foundDiagnostics.push(
          new vscode.Diagnostic(ref.range, `${ref.macro} path resolves to a file, expected a directory.`, vscode.DiagnosticSeverity.Error),
        );
      } else if (isModuleDirectoryKind(ref.kind)) {
        const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
        const owner = await findOwnerYaMake(ref.target, workspaceFolder && workspaceFolder.uri);
        if (!owner) {
          foundDiagnostics.push(
            new vscode.Diagnostic(ref.range, `${ref.macro} path does not contain ya.make: ${ref.displayTarget}`, vscode.DiagnosticSeverity.Warning),
          );
        }
      }
    } catch (error) {
      foundDiagnostics.push(
        new vscode.Diagnostic(ref.range, `Path does not exist: ${ref.displayTarget}`, vscode.DiagnosticSeverity.Error),
      );
    }
  }

  diagnostics.set(document.uri, foundDiagnostics);
}

function findMacroAtPosition(document, position) {
  const offset = document.offsetAt(position);
  const parsed = parseMakelist(document.getText());

  for (const call of parsed.calls) {
    if (call.nameStart <= offset && offset <= call.nameEnd && MACRO_DOCS[call.name]) {
      return call.name;
    }
  }

  return undefined;
}

function formatMacroHover(macroName) {
  const doc = MACRO_DOCS[macroName];
  const markdown = new vscode.MarkdownString(undefined, true);
  markdown.appendCodeblock(doc.usage, "yamake");
  markdown.appendMarkdown(doc.text);
  return markdown;
}

module.exports = {
  activate,
  deactivate,
};
