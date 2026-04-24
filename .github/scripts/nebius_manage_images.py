import os
import asyncio
import argparse
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ManageImagesOptions:
    github_token: str
    github_repository: str
    new_image_id: str | None
    image_variable_name: str
    update_image_id: bool
    parent_id: str
    images_to_keep: int
    remove_old_images: bool


def build_image_family_name(github_repository: str) -> str:
    github_repository_owner, github_repository_name = github_repository.split("/")
    return f"github-runner-{github_repository_owner}-{github_repository_name}"


def resolve_new_image_id(current_image_id: str, new_image_id: str | None) -> str:
    return new_image_id or current_image_id


def fetch_repo_variable(github_client, github_repository: str, image_variable_name: str):
    repo = github_client.get_repo(github_repository)
    return repo, repo.get_variable(image_variable_name)


def log_image_inventory(
    images: list[Image], image_family_name: str, new_image_id: str
) -> list[Image]:
    candidate_images: list[Image] = []

    for image in images:
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
            "Found image%s: %s %s %s %s %s %s %s %s%s",
            # fmt: off
            prefix, image.metadata.id, image.metadata.name, status, created_at,
            image.spec.image_family, storage_size, min_disk_size,
            ', '.join(f"{k}={v}" for k, v in image.metadata.labels.items()),
            postfix,
            # fmt: on
        )

    return candidate_images


def select_images_to_remove(
    candidate_images: list[Image], images_to_keep: int
) -> list[Image]:
    return candidate_images[images_to_keep:]


async def list_ready_images(
    service: ImageServiceClient, parent_id: str, image_family_name: str
) -> list[Image]:
    request = ListImagesRequest(
        parent_id=parent_id,
        filter=f"family IN ('{image_family_name}') AND status = 'READY'",
    )
    images: list[Image] = []
    try:
        while True:
            response = await service.list(request)
            images.extend(response.items)
            if not response.next_page_token:
                break
            request.page_token = response.next_page_token
    except RequestError as e:
        logger.info("Result: %s", images)
        logger.error("Failed to list images", exc_info=True)
        raise e

    return images


async def remove_images(service: ImageServiceClient, images_to_remove: list[Image]) -> None:
    for image in images_to_remove:
        logger.info("Removing image %s", image.metadata.id)
        request = DeleteImageRequest(id=image.metadata.id)
        response = await service.remove(request)
        if response.error:
            logger.error(
                "Failed to remove image %s: %s", image.metadata.id, response.error
            )
        else:
            logger.info("Image %s removed", image.metadata.id)


async def main(
    sdk: SDK,
    options: ManageImagesOptions,
    github_client_factory=Github,
    image_service_factory=ImageServiceClient,
) -> None:
    github = github_client_factory(options.github_token)
    _, variable = fetch_repo_variable(
        github, options.github_repository, options.image_variable_name
    )
    image_family_name = build_image_family_name(options.github_repository)

    logger.info("%s (old) = %s", options.image_variable_name, variable.value)

    new_image_id = resolve_new_image_id(variable.value, options.new_image_id)

    if options.update_image_id:
        variable.edit(value=new_image_id)
        logger.info("%s (new) = %s", options.image_variable_name, new_image_id)
    else:
        logger.info("Would set %s (new) = %s", options.image_variable_name, new_image_id)

    service = image_service_factory(sdk)
    images = await list_ready_images(service, options.parent_id, image_family_name)
    candidate_images = log_image_inventory(images, image_family_name, new_image_id)

    logger.info("Total images before cleanup: %d", len(images))
    if not options.remove_old_images:
        logger.info("Old images will not be removed")
        return

    images_to_remove = select_images_to_remove(candidate_images, options.images_to_keep)
    images_to_remove_ids = [image.metadata.id for image in images_to_remove]
    logger.info("Images selected for removal: %s", images_to_remove_ids)
    await remove_images(service, images_to_remove)
    logger.info("Total images after cleanup: %d", len(candidate_images) - len(images_to_remove))


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
            options=ManageImagesOptions(
                github_token=args.github_token,
                github_repository=args.github_repo,
                new_image_id=args.new_image_id,
                image_variable_name=args.image_variable_name,
                update_image_id=args.update_image_id,
                parent_id=args.parent_id,
                images_to_keep=args.images_to_keep,
                remove_old_images=args.remove_old_images,
            ),
        )
    )
