module.exports = async function checkOwnershipMembership({ github, context, core }) {
  async function isOrgMember(owner, username) {
    core.info(`Checking membership for user: ${username}`);
    try {
      const { data: membership } = await github.rest.orgs.getMembershipForUser({
        org: owner,
        username,
      });
      if (membership?.state === "active") {
        core.info(`${username} is confirmed as an org member`);
        return true;
      }

      core.info(`${username} is not an active org member (state: ${membership?.state})`);
      return false;
    } catch (error) {
      // The API commonly errors when the user is not in the org or membership is private.
      core.error(`Error checking membership for user ${username}: ${error.message}`);
      return false;
    }
  }

  const { owner, repo } = context.repo;
  const prNumber = context.payload.pull_request.number;
  const prAuthor = context.payload.pull_request.user.login;

  core.info(`Starting membership check for PR #${prNumber} by @${prAuthor}`);

  const authorIsOrgMember = await isOrgMember(owner, prAuthor);
  if (authorIsOrgMember) {
    core.info(`User @${prAuthor} is org member => authorized`);
    return true;
  }

  core.info(`User @${prAuthor} is NOT an org member; checking for 'ok-to-test' label`);

  let events;
  try {
    const resp = await github.rest.issues.listEvents({
      owner,
      repo,
      issue_number: prNumber,
    });
    events = resp.data;
  } catch (error) {
    core.error(`Error fetching issue events: ${error.message}`);
    return false;
  }

  const labeledOkToTest = events
    .filter((event) => event.event === "labeled" && event.label?.name === "ok-to-test")
    .pop();

  if (!labeledOkToTest) {
    core.info("No 'ok-to-test' label found on this PR");
    return false;
  }

  core.info(
    `Found 'ok-to-test' label event by @${labeledOkToTest.actor.login}, verifying if they are an org member...`,
  );

  const labelerLogin = labeledOkToTest.actor.login;
  const labelerIsOrgMember = await isOrgMember(owner, labelerLogin);
  if (!labelerIsOrgMember) {
    core.info(`User @${labelerLogin} who labeled 'ok-to-test' is not an org member => not authorized`);
    return false;
  }

  core.info(`'ok-to-test' label added by an org member => authorized`);
  return true;
};
