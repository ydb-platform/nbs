import asyncio
from types import SimpleNamespace

from . import nebius_manage_images as m


def make_image(
    image_id: str,
    *,
    family: str = m.IMAGE_FAMILY_NAME,
    status: str = "READY",
    created_at: str = "2026-04-24T10:00:00Z",
    labels: dict | None = None,
):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            id=image_id,
            name=f"name-{image_id}",
            created_at=created_at,
            labels=labels or {"test": "librarian"},
        ),
        spec=SimpleNamespace(image_family=family),
        status=SimpleNamespace(
            state=SimpleNamespace(name=status),
            storage_size_bytes=1024,
            min_disk_size_bytes=2048,
        ),
    )


def test_resolve_new_image_id_falls_back_to_current_value():
    assert m.resolve_new_image_id("current-image-id", None) == "current-image-id"


def test_log_image_inventory_returns_only_removable_images():
    protected_image = make_image(m.PROTECTED_IMAGE_IDS[0])
    current_image = make_image("current-image")
    other_family_image = make_image("other-family", family="different-family")
    non_ready_image = make_image("building-image", status="CREATING")
    candidate_image = make_image("candidate-image")

    candidate_images = m.log_image_inventory(
        [
            protected_image,
            current_image,
            other_family_image,
            non_ready_image,
            candidate_image,
        ],
        m.IMAGE_FAMILY_NAME,
        "current-image",
    )

    assert [image.metadata.id for image in candidate_images] == ["candidate-image"]


def test_select_images_to_remove_keeps_requested_count():
    candidate_images = [
        make_image("keep-1"),
        make_image("keep-2"),
        make_image("remove-1"),
        make_image("remove-2"),
    ]

    images_to_remove = m.select_images_to_remove(candidate_images, images_to_keep=2)

    assert [image.metadata.id for image in images_to_remove] == [
        "remove-1",
        "remove-2",
    ]


def test_list_ready_images_collects_all_pages():
    class FakeImageService:
        def __init__(self):
            self.requests = []

        async def list(self, request):
            self.requests.append(
                SimpleNamespace(
                    parent_id=request.parent_id,
                    page_token=request.page_token,
                )
            )
            if len(self.requests) == 1:
                return SimpleNamespace(
                    items=[make_image("page-1")], next_page_token="page-2"
                )
            return SimpleNamespace(items=[make_image("page-2")], next_page_token="")

    service = FakeImageService()

    images = asyncio.run(
        m.list_ready_images(
            service=service,
            parent_id="project-id",
            image_family_name=m.IMAGE_FAMILY_NAME,
        )
    )

    assert [image.metadata.id for image in images] == ["page-1", "page-2"]
    assert service.requests[0].parent_id == "project-id"
    assert service.requests[0].page_token == ""
    assert service.requests[1].page_token == "page-2"


def test_main_updates_variable_and_removes_old_images():
    class FakeVariable:
        def __init__(self, value):
            self.value = value
            self.edits = []

        def edit(self, *, value):
            self.edits.append(value)
            self.value = value

    class FakeRepo:
        def __init__(self, variable):
            self.variable = variable

        def get_variable(self, name):
            self.variable_name = name
            return self.variable

    class FakeGithub:
        def __init__(self, variable):
            self.variable = variable
            self.repositories = []

        def get_repo(self, repository):
            self.repositories.append(repository)
            return FakeRepo(self.variable)

    class FakeImageService:
        def __init__(self, _sdk):
            self.sdk = _sdk
            self.removed_ids = []

        async def list(self, request):
            self.request = request
            return SimpleNamespace(
                items=[
                    make_image("new-image"),
                    make_image("keep-image"),
                    make_image("remove-image"),
                ],
                next_page_token="",
            )

        async def remove(self, request):
            self.removed_ids.append(request.id)
            return SimpleNamespace(error=None)

    variable = FakeVariable("old-image")
    github_client = FakeGithub(variable)
    created_services = []

    def github_client_factory(_token):
        assert _token == "github-token"
        return github_client

    def image_service_factory(sdk):
        service = FakeImageService(sdk)
        created_services.append(service)
        return service

    asyncio.run(
        m.main(
            sdk=object(),
            options=m.ManageImagesOptions(
                github_token="github-token",
                github_repository="ydb-platform/nbs",
                new_image_id="new-image",
                image_variable_name="NEBIUS_IMAGE_ID_2204",
                update_image_id=True,
                parent_id="project-id",
                images_to_keep=1,
                remove_old_images=True,
            ),
            github_client_factory=github_client_factory,
            image_service_factory=image_service_factory,
        )
    )

    assert github_client.repositories == ["ydb-platform/nbs"]
    assert variable.edits == ["new-image"]
    assert created_services[0].removed_ids == ["remove-image"]
