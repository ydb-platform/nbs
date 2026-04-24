import os
import asyncio
import argparse
from .helpers import setup_logger, convert_size
from github import Github
from nebius.sdk import SDK
from nebius.aio.cli_config import Config
from nebius.aio.service_error import RequestError
from nebius.api.nebius.compute.v1 import Image
from nebius.api.nebius.compute.v1 import (
    DeleteImageRequest,
    ListImagesRequest,
    ImageServiceClient,
)

logger = setup_logger()

# they won't appear in the list of images
# but they will be protected from deletion anyway
PROTECTED_IMAGE_IDS = [
    "computeimage-e00f4cy4p43wx8gm4v",
]


async def main(
    sdk: SDK,
    github_token: str,
    github_repository: str,
    new_image_id: str,
    image_variable_name: str,
    update_image_id: bool,
    parent_id: str,
    images_to_keep: int,
    remove_old_images: bool,
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

    service = ImageServiceClient(sdk)
    request = ListImagesRequest(
        parent_id=parent_id,
        filter=f"family IN ('{image_family_name}') AND status = 'READY'",
    )
    result: list[Image] = []
    try:
        while True:
            response = await service.list(request)
            result.extend(response.items)
            if not response.next_page_token:
                break
            request.page_token = response.next_page_token
    except RequestError as e:
        logger.info("Result: %s", result)
        logger.error("Failed to list images", exc_info=True)
        raise e

    candidate_images: list[Image] = []
    for image in result:
        status = image.status.state.name
        created_at = image.metadata.created_at
        storage_size = convert_size(image.status.storage_size_bytes)
        min_disk_size = convert_size(image.status.min_disk_size_bytes)
        prefix = " (PROTECTED)"
        postfix = ""

        if image.metadata.id == new_image_id:
            postfix = " (NEW)"

        if (
            image.metadata.id not in PROTECTED_IMAGE_IDS
            and image.spec.image_family == image_family_name  # noqa: W503
            and image.metadata.id != new_image_id  # noqa: W503
        ):
            prefix = ""
            postfix += " (SELECTED)"
            candidate_images.append(image)

        logger.info(
            "Found image%s: %s %s %s %s %s %s %s %s",
            # fmt: off
            prefix, image.metadata.id, image.metadata.name, status, created_at,
            image.spec.image_family, storage_size, min_disk_size,
            ', '.join(f"{k}={v}" for k, v in image.metadata.labels.items())
            # fmt: on
        )
    logger.info("Total images before cleanup: %d", len(response.items))
    if not remove_old_images:
        logger.info("Old images will not be removed")
        return

    images_to_remove = candidate_images[images_to_keep:]
    images_to_remove_ids = [image.metadata.id for image in images_to_remove]
    logger.info("Images selected for removal: %s", images_to_remove_ids)

    for image in images_to_remove:
        logger.info("Removing image %s", image.id)
        request = DeleteImageRequest(image_id=image.metadata.id)
        response = await service.remove(request)
        if response.error:
            logger.error("Failed to remove image %s: %s", image.id, response.error)
        else:
            logger.info("Image %s removed", image.id)
            candidate_images.remove(image)

    logger.info("Total images after cleanup: %d", len(candidate_images))


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
        "--parent-id",
        default="project-e00p3n19h92mcw7vke",
        help="Parent ID where the VM will be created",
    )
    parser.add_argument(
        "--new-image-id",
        default=os.getenv("NEW_IMAGE_ID"),
        help="ID of the new image to use",
    )
    parser.add_argument(
        "--image-variable-name",
        default="NEBIUS_IMAGE_ID_2204",
        help="Name of the variable to update in GitHub Actions",
    )
    parser.add_argument(
        "--update-image-id",
        default=False,
        action="store_true",
        help="Update the image id in Github Actions variables",
    )
    parser.add_argument(
        "--images-to-keep", default=7, type=int, help="Number of images to keep"
    )
    parser.add_argument(
        "--remove-old-images",
        default=False,
        action="store_true",
        help="Remove old images",
    )

    args = parser.parse_args()
    logger.info(args)

    sdk = SDK(config_reader=Config())
    asyncio.run(
        main(
            sdk=sdk,
            github_token=args.github_token,
            github_repository=args.github_repo,
            new_image_id=args.new_image_id,
            image_variable_name=args.image_variable_name,
            update_image_id=args.update_image_id,
            parent_id=args.parent_id,
            images_to_keep=args.images_to_keep,
            remove_old_images=args.remove_old_images,
        )
    )
