import os
import json
import grpc
import math
import logging
import argparse
from github import Github
from datetime import datetime, timezone
from yandex.cloud.compute.v1.image_pb2 import Image
from yandex.cloud.compute.v1.image_service_pb2 import (
    DeleteImageRequest,
    ListImagesRequest,
)
from yandex.cloud.compute.v1.image_service_pb2_grpc import ImageServiceStub
from yandexcloud import SDK, RetryInterceptor, backoff_linear_with_jitter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# they won't appear in the list of images
# but they will be protected from deletion anyway
PROTECTED_IMAGE_IDS = [
    "arl2kdkt7j8aorb1ib1h",
    "arlf5b2mm2k1s4b71u4t",
    "arlhviu5hj9aj6uqq6l3",
    "arlpgqq65s5p13srj592",
    "arl7p6s8d25e8m83kvi3",
]


def status_to_string(image: Image) -> str:
    return Image.Status.Name(image.status)


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = math.floor(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"


def main(
    sdk: SDK,
    github_token: str,
    github_repository: str,
    new_image_id: str,
    image_variable_name: str,
    update_image_id: bool,
    images_to_keep: int,
    remove_old_images: bool,
    folder_id: str,
) -> None:
    github = Github(github_token)
    repo = github.get_repo(github_repository)
    github_repository_owner, github_repository_name = github_repository.split("/")
    image_family_name = (
        f"github-runner-{github_repository_owner}-{github_repository_name}"
    )

    variable = repo.get_variable(image_variable_name)

    logger.info("%s (old) = %s" % (image_variable_name, variable.value))

    if new_image_id is None:
        new_image_id = variable.value

    if update_image_id:
        variable.edit(value=new_image_id)
        logger.info("%s (new) = %s", image_variable_name, new_image_id)
    else:
        logger.info("Would set %s (new) = %s", image_variable_name, new_image_id)

    request = ListImagesRequest(
        folder_id=folder_id,
        order_by="created_at desc",
        filter=f"family IN ('{image_family_name}') AND status = 'READY'",
    )

    client = sdk.client(ImageServiceStub)
    response = client.List(request)
    candidate_images = []
    for image in response.images:
        status = status_to_string(image)
        created_at = datetime.fromtimestamp(image.created_at.seconds, tz=timezone.utc)
        storage_size = convert_size(image.storage_size)
        min_disk_size = convert_size(image.min_disk_size)
        prefix = " (PROTECTED)"
        postfix = ""

        if image.id == new_image_id:
            postfix = " (NEW)"

        if (
            image.id not in PROTECTED_IMAGE_IDS
            and image.family == image_family_name  # noqa: W503
            and image.id != new_image_id  # noqa: W503
        ):
            prefix = ""
            postfix += " (SELECTED)"
            candidate_images.append(image)

        logger.info(
            "Found image%s: %s %s %s %s %s %s %s %s",
            # fmt: off
            prefix, image.id, image.name, status, created_at,
            image.family, storage_size, min_disk_size,
            ', '.join(f"{k}={v}" for k, v in image.labels.items())
            # fmt: on
        )
    logger.info("Total images: %d", len(response.images))

    for image in candidate_images[images_to_keep:]:
        if remove_old_images:
            logger.info("Removing image %s", image.id)
            client.Delete(DeleteImageRequest(image_id=image.id))
            logger.info("Image %s removed", image.id)
        else:
            logger.info("Would remove image %s", image.id)


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
        "--api-endpoint",
        default="api.ai.nebius.cloud",
        help="Cloud API Endpoint",
    )
    parser.add_argument(
        "--folder-id",
        default="bjeuq5o166dq4ukv3eec",
        help="Folder ID where the VM will be created",
    )
    parser.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
    )
    parser.add_argument(
        "--new-image-id",
        default=os.getenv("NEW_IMAGE_ID"),
        help="Name of the artifact to download and extract",
    )
    parser.add_argument(
        "--image-variable-name",
        default="IMAGE_ID_2204",
        help="Name of the variable to update in GitHub Actions",
    )
    parser.add_argument(
        "--update-image-id",
        default=False,
        action="store_true",
        help="Update the image id in Github Actions variables",
    )
    parser.add_argument(
        "--images-to-keep", default=7, type=int, help="Workflow file name"
    )

    parser.add_argument(
        "--remove-old-images",
        default=False,
        action="store_true",
        help="Remove old images",
    )

    args = parser.parse_args()
    logger.info(args)

    interceptor = RetryInterceptor(
        max_retry_count=30,
        retriable_codes=[grpc.StatusCode.UNAVAILABLE],
        back_off_func=backoff_linear_with_jitter(5, 0),
    )

    with open(args.service_account_key, "r") as fp:
        sdk = SDK(
            service_account_key=json.load(fp),
            endpoint=args.api_endpoint,
            interceptor=interceptor,
        )

    main(
        sdk=sdk,
        github_token=args.github_token,
        github_repository=args.github_repo,
        new_image_id=args.new_image_id,
        image_variable_name=args.image_variable_name,
        update_image_id=args.update_image_id,
        images_to_keep=args.images_to_keep,
        remove_old_images=args.remove_old_images,
        folder_id=args.folder_id,
    )
