const vscode = require("vscode");

const { RESOURCE_MACROS, RESOURCE_MAPPING_FILES } = require("./macroMetadata");
const { parseMakelist, positionAtOffset, stripQuotes } = require("./makelistParser");

let resourceMappingCache;

function clearResourceMappingCache() {
  resourceMappingCache = undefined;
}

function isResourceMappingDocument(document) {
  return RESOURCE_MAPPING_FILES.some((relativePath) => document.uri.path.endsWith(`/${relativePath}`));
}

function findResourceRefAtPosition(document, position) {
  const offset = document.offsetAt(position);
  const parsed = parseMakelist(document.getText());

  for (const call of parsed.calls) {
    if (!RESOURCE_MACROS.has(call.name)) {
      continue;
    }
    for (const arg of call.args) {
      if (arg.start <= offset && offset <= arg.end && /^\d+$/.test(stripQuotes(arg.value))) {
        return {
          macro: call.name,
          value: stripQuotes(arg.value),
          range: new vscode.Range(document.positionAt(arg.start), document.positionAt(arg.end)),
        };
      }
    }
  }

  return undefined;
}

function formatResourceHover(resourceId, mapping) {
  const markdown = new vscode.MarkdownString(undefined, true);
  markdown.appendMarkdown(`**Resource \`${resourceId}\`**\n\n`);
  markdown.appendMarkdown(`Declared in \`${mapping.relativePath}\`.\n\n`);
  if (mapping.description) {
    markdown.appendMarkdown(`${mapping.description}\n\n`);
  }
  markdown.appendMarkdown(mapping.url);
  return markdown;
}

async function findResourceMapping(document, resourceId) {
  const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
  if (!workspaceFolder) {
    return undefined;
  }

  const mappings = await loadResourceMappings(workspaceFolder.uri);
  return mappings.get(resourceId);
}

async function loadResourceMappings(workspaceUri) {
  const workspaceKey = workspaceUri.toString();
  if (resourceMappingCache && resourceMappingCache.workspaceKey === workspaceKey) {
    return resourceMappingCache.mappings;
  }

  const mappings = new Map();
  for (const relativePath of RESOURCE_MAPPING_FILES) {
    const uri = joinUri(workspaceUri, relativePath);
    let text;
    try {
      const bytes = await vscode.workspace.fs.readFile(uri);
      text = Buffer.from(bytes).toString("utf8");
    } catch (error) {
      continue;
    }

    let data;
    try {
      data = JSON.parse(text);
    } catch (error) {
      continue;
    }

    const resources = data.resources || {};
    const descriptions = data.resources_descriptions || {};
    for (const [id, url] of Object.entries(resources)) {
      if (mappings.has(id)) {
        continue;
      }
      const keyOffset = text.indexOf(`"${id}"`);
      const start = keyOffset >= 0 ? keyOffset + 1 : 0;
      const end = keyOffset >= 0 ? start + id.length : 0;
      mappings.set(id, {
        id,
        url,
        description: descriptions[id],
        relativePath,
        uri,
        location: new vscode.Location(uri, new vscode.Range(positionAtOffset(text, start), positionAtOffset(text, end))),
      });
    }
  }

  resourceMappingCache = { workspaceKey, mappings };
  return mappings;
}

function joinUri(base, relativePath) {
  const parts = relativePath.split("/").filter(Boolean);
  return vscode.Uri.joinPath(base, ...parts);
}

module.exports = {
  clearResourceMappingCache,
  findResourceMapping,
  findResourceRefAtPosition,
  formatResourceHover,
  isResourceMappingDocument,
};
