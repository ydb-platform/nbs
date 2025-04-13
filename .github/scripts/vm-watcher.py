import os
import grpc
import json
import logging
import argparse
from github import Github, Auth as GithubAuth
from datetime import datetime, timedelta, timezone
from yandexcloud import SDK, RetryInterceptor
from yandex.cloud.compute.v1.instance_service_pb2_grpc import InstanceServiceStub
from yandex.cloud.compute.v1.instance_service_pb2 import (
    ListInstancesRequest,
    DeleteInstanceRequest,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

CACHE_VM_ID = "dp7329odurnhplpf5ff0"


def find_workflows_containing_string(
    client, specified_time, search_string, owner="ydb-platform", repo="nbs"
):
    repo = client.get_repo(f"{owner}/{repo}")

    # Calculate start time for search (-10 minutes)
    start_time = specified_time - timedelta(minutes=10)
    end_time = specified_time + timedelta(minutes=10)

    # Initialize list to keep track of matching runs
    matching_runs_info = []

    for run in repo.get_workflow_runs():
        # Check if the run started within our time window
        run_started_at = run.created_at
        if start_time <= run_started_at <= end_time:
            print("Workflow", run.name, run.created_at, run.html_url)
            # Get jobs or the current workflow run
            for job in run.jobs():

                if "Start self-hosted runner" in job.name:
                    print("Job", job.name)
                    # Attempt to get logs (note: this might require additional handling for large logs)
                    try:
                        logs = job.get_log()
                        if search_string in logs:
                            matching_runs_info.append(
                                {
                                    "run_id": run.id,
                                    "run_url": run.html_url,  # Link to the workflow run
                                    "job_name": job.name,
                                }
                            )
                    except Exception as e:
                        print(
                            f"Error fetching logs for job {job.name} in run {run.id}: {e}"
                        )

    return matching_runs_info


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
    )
    parser.add_argument(
        "--folder-id",
        required=True,
        help="The ID of the folder to list instances in",
        default="bjeuq5o166dq4ukv3eec",
    )
    parser.add_argument(
        "--ttl", required=True, help="The TTL for the VMs", default=24, type=int
    )
    parser.add_argument("--apply", action="store_true", help="Apply the changes")

    args = parser.parse_args()

    threshold = datetime.now() - timedelta(hours=args.ttl)

    interceptor = RetryInterceptor(
        max_retry_count=5, retriable_codes=[grpc.StatusCode.UNAVAILABLE]
    )

    with open(args.service_account_key, "r") as fp:
        sdk = SDK(
            service_account_key=json.load(fp),
            endpoint="api.ai.nebius.cloud",
            interceptor=interceptor,
        )

    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))

    client = sdk.client(InstanceServiceStub)
    response = client.List(ListInstancesRequest(folder_id=args.folder_id))

    for vm in response.instances:
        if vm.id == CACHE_VM_ID:
            logging.info(f"Skipping VM {vm.id} as it is a cache VM")
            continue

        creation_time = vm.created_at.ToDatetime()
        if creation_time < threshold:
            logging.info(
                f"VM {vm.id} is older than 24 hours, deleting it, created at {creation_time}"
            )

            if args.apply:
                client.Delete(DeleteInstanceRequest(instance_id=vm.id))
            else:
                runs = find_workflows_containing_string(
                    gh, creation_time.replace(tzinfo=timezone.utc), vm.id
                )
                print("Runs that match this id", runs)

        else:
            logging.info(
                f"VM {vm.id} is younger than 24 hours, keeping it, created at {creation_time}"
            )
