from scripts import nightly_pr_check as n


class FakeWorkflowRun:
    def __init__(self, accepted: bool):
        self.accepted = accepted
        self.cancelled = False

    def cancel(self) -> bool:
        self.cancelled = True
        return self.accepted


class FakeRepo:
    full_name = "owner/repo"

    def __init__(self, workflow_run: FakeWorkflowRun):
        self.workflow_run = workflow_run
        self.requested_run_id = None

    def get_workflow_run(self, run_id: int) -> FakeWorkflowRun:
        self.requested_run_id = run_id
        return self.workflow_run


def test_cancel_workflow_run_uses_pygithub_workflow_run_cancel() -> None:
    workflow_run = FakeWorkflowRun(accepted=True)
    repo = FakeRepo(workflow_run)

    result = n.cancel_workflow_run(repo, 123)

    assert repo.requested_run_id == 123
    assert workflow_run.cancelled
    assert result == n.CancelRequestResult(
        accepted=True,
        status_code=202,
        debug="WorkflowRun.cancel returned accepted=True",
    )


def test_cancel_workflow_run_reports_not_accepted() -> None:
    result = n.cancel_workflow_run(FakeRepo(FakeWorkflowRun(accepted=False)), 123)

    assert result == n.CancelRequestResult(
        accepted=False,
        status_code=0,
        debug="WorkflowRun.cancel returned accepted=False",
    )
