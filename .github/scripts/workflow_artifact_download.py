import os
import logging
import requests
import zipfile
import io
from github import Github
from typing import List, Optional
import argparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


def download_and_extract_artifact(
    artifact_url: str, headers: dict, files_to_extract: List[str], output_path: str
) -> List[str]:
    extracted_files = []
    response = requests.get(artifact_url, headers=headers, stream=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        if files_to_extract:
            for file_name in files_to_extract:
                try:
                    zip_file.extract(file_name, path=output_path)
                    extracted_files.append(file_name)
                    log_file_extraction(file_name, output_path)
                except KeyError:
                    logger.error("File '%s' not found within the artifact.", file_name)
        else:
            zip_file.extractall(path=output_path)
            extracted_files = zip_file.namelist()
            for file_name in extracted_files:
                log_file_extraction(file_name, output_path)
    return extracted_files


def log_file_extraction(file_name: str, output_path: str) -> None:
    file_path = os.path.join(output_path, file_name)
    logger.info("Extracted '%s' to '%s'", file_name, file_path)


def write_contents_to_output(
    output_file_path: str, output_path: str, files: List[str], run_id: int = 0
) -> None:
    with open(output_file_path, "a") as output_file:
        for file_name in files:
            file_path = os.path.join(output_path, file_name)
            try:
                with open(file_path, "r") as f:
                    content = f.read()
                    normalized_file_name = file_name.replace(".", "_").replace("-", "_")
                    # Here we use the filename as the variable name for simplicity
                    logger.info(
                        "Writing %s=%s to %s",
                        normalized_file_name,
                        content,
                        output_file_path,
                    )
                    output_file.write(f"{normalized_file_name}={content}\n")
                    output_file.write(f"latest_url={content}\n")
                    if run_id != 0:
                        output_file.write(f"run_id={run_id}\n")
            except FileNotFoundError:
                logger.error(
                    "File '%s' was not found in the extracted files.", file_name
                )


def main(
    github_token: str,
    github_repository: str,
    output_path: str,
    artifact_name: str,
    workflow_name: str,
    workflow_run_id: int,
    branch: str,
    files_to_extract: Optional[List[str]],
    github_output: str,
) -> None:
    g = Github(github_token)
    repo = g.get_repo(github_repository)
    if workflow_run_id != 0:
        workflow_runs = [repo.get_workflow_run(workflow_run_id)]
    else:
        workflow = repo.get_workflow(workflow_name)
        workflow_runs = workflow.get_runs(branch=branch, status="success")

    for run in workflow_runs:
        artifacts = run.get_artifacts()
        for artifact in artifacts:
            if artifact.name == artifact_name:
                logger.info("Found artifact '%s' in run %d", artifact_name, run.id)
                headers = {"Authorization": f"token {github_token}"}
                extracted_files = download_and_extract_artifact(
                    artifact.archive_download_url,
                    headers,
                    files_to_extract,
                    output_path,
                )

                if files_to_extract is None:
                    # If no specific files were requested, write all extracted files' contents
                    files_to_extract = extracted_files

                write_contents_to_output(
                    github_output, output_path, files_to_extract, run.id
                )
                logger.info("Artifact contents written to '%s'", github_output)
                return
    logger.info(
        "Artifact named '%s' not found in any workflow runs on branch '%s'.",
        artifact_name,
        branch,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and process GitHub Actions artifact."
    )
    parser.add_argument(
        "--github-token",
        default=os.getenv("GITHUB_TOKEN"),
        help="GitHub Token for authentication",
    )
    parser.add_argument(
        "--github-repo",
        default=os.getenv("GITHUB_REPOSITORY"),
        help='GitHub repository in the format "owner/repo"',
    )
    parser.add_argument(
        "--output",
        default=os.getenv("GITHUB_WORKSPACE", "."),
        help="Output path for the extracted files",
    )
    parser.add_argument(
        "--github-output",
        default=os.getenv("GITHUB_OUTPUT"),
        help="File path for writing output variables for GitHub Actions",
    )
    parser.add_argument(
        "--artifact",
        default="ya_web_url_file",
        help="Name of the artifact to download and extract",
    )
    parser.add_argument("--workflow", default="nightly.yaml", help="Workflow file name")
    parser.add_argument(
        "--workflow-run-id",
        default=0,
        type=int,
        help="Workflow run id (if we want specific run)",
    )
    parser.add_argument("--branch", default="main", help="Repository branch")
    parser.add_argument(
        "--files",
        nargs="*",
        help="List of files to extract from the artifact. If not specified, all files are extracted.",
    )

    args = parser.parse_args()

    logger.info(args)

    main(
        github_token=args.github_token,
        github_repository=args.github_repo,
        output_path=args.output,
        artifact_name=args.artifact,
        workflow_name=args.workflow,
        workflow_run_id=args.workflow_run_id,
        branch=args.branch,
        files_to_extract=args.files,
        github_output=args.github_output,
    )
