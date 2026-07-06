import pytest

from . import mirror_yatool_resources as mirror


def test_validate_source_url_accepts_devtools_registry_resource() -> None:
    url = "https://devtools-registry.s3.yandex.net/6277415836"

    assert mirror.validate_source_url("6277415836", url) == url


@pytest.mark.parametrize(
    "resource_id,url",
    [
        ("6277415836", "file:///home/github/.aws/credentials"),
        ("6277415836", "http://devtools-registry.s3.yandex.net/6277415836"),
        ("6277415836", "https://169.254.169.254/latest/meta-data/"),
        ("6277415836", "https://devtools-registry.s3.yandex.net/other"),
        ("6277415836", "https://devtools-registry.s3.yandex.net/6277415836?x=1"),
        ("6277415836", "https://user@devtools-registry.s3.yandex.net/6277415836"),
        ("not-numeric", "https://devtools-registry.s3.yandex.net/not-numeric"),
    ],
)
def test_validate_source_url_rejects_untrusted_urls(resource_id: str, url: str) -> None:
    with pytest.raises(ValueError):
        mirror.validate_source_url(resource_id, url)
