DECLARE_IN_DIRS(DOCS *.*m? DIRS . RECURSIVE SRCDIR ${ARCADIA_ROOT}/contrib/ydb/docs) # *.md or *.yaml
RUN_PROGRAM(
    contrib/ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate ${ARCADIA_ROOT}/contrib/ydb/docs/ ${BINDIR}/ydb_docs.archive
    IN ${DOCS_FILES}
    OUT_NOAUTO ${BINDIR}/ydb_docs.archive
)
RESOURCE(ydb_docs.archive ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate/docs.archive)
