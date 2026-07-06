module.exports = async function checkTeamLabel({
  github,
  context,
  core,
  labelName,
  teamSlugs,
}) {
  const { owner, repo } = context.repo;
  const prNumber = context.payload.pull_request.number;
  const currentLabels = new Set(
    context.payload.pull_request.labels.map((label) => label.name),
  );

  if (!currentLabels.has(labelName)) {
    core.info(`Label '${labelName}' is not currently present on PR #${prNumber}`);
    return false;
  }

  const teamMembers = new Set();
  for (const teamSlug of teamSlugs) {
    core.info(`Loading members of team ${owner}/${teamSlug}`);
    const members = await github.paginate(github.rest.teams.listMembersInOrg, {
      org: owner,
      team_slug: teamSlug,
      per_page: 100,
    });
    for (const member of members) {
      teamMembers.add(member.login.toLowerCase());
    }
  }

  const events = await github.paginate(github.rest.issues.listEvents, {
    owner,
    repo,
    issue_number: prNumber,
    per_page: 100,
  });
  const labeledEvent = events
    .filter((event) => event.event === "labeled" && event.label?.name === labelName)
    .pop();

  if (!labeledEvent) {
    core.info(`No label event found for '${labelName}' on PR #${prNumber}`);
    return false;
  }

  const actor = labeledEvent.actor.login;
  const allowed = teamMembers.has(actor.toLowerCase());
  core.info(
    `Latest '${labelName}' label was added by @${actor}; team member=${allowed}`,
  );
  return allowed;
};
