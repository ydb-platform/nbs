import contextlib
from collections import defaultdict
from datetime import datetime, timedelta
from typing import NamedTuple, Type

from cloud.disk_manager.test.acceptance.test_runner.\
    base_acceptance_test_runner import cleanup_previous_acceptance_tests_results
from cloud.disk_manager.test.acceptance.test_runner.acceptance_test_runner\
    import AcceptanceTestCleaner
from cloud.disk_manager.test.acceptance.test_runner.\
    eternal_acceptance_test_runner import EternalTestCleaner
from cloud.disk_manager.test.acceptance.test_runner.sync_acceptance_test_runner\
    import SyncTestCleaner


class _YcpMock:
    def __init__(
        self,
        resources: list['_Resource'],
        exception_type: Type[Exception],
    ):
        self._resources = defaultdict(list)
        for resource in resources:
            self._resources[resource.resource_type + 's'].append(resource)
        self._recorded_deletions = []
        self.should_raise_error = False
        self._exception_type = exception_type

    def __getattr__(self, item):
        items_split = item.split('_')
        if len(items_split) != 2:
            raise AttributeError()
        [operation, resource_name] = items_split

        def _list():
            if self.should_raise_error:
                raise self._exception_type()
            return self._resources[resource_name]

        def _delete(resource):
            if self.should_raise_error:
                raise self._exception_type()
            self._recorded_deletions.append(resource)

        if operation == 'list':
            try:
                return _list
            except KeyError:
                raise AttributeError()
        elif operation == 'delete':
            return _delete
        raise AttributeError()

    @property
    def recorded_deletions(self) -> list['_Resource']:
        return self._recorded_deletions

    @contextlib.contextmanager
    def restoring_recorded_deletion(self):
        try:
            yield
        finally:
            self._recorded_deletions = []


class _Resource(NamedTuple):

    name: str
    resource_type: str
    created_at: datetime

    def __hash__(self):
        return (
            hash(self.name) +
            hash(self.created_at) +
            hash(self.resource_type)
        )

    def __eq__(self, other):
        if self.name != other.name:
            return False
        if self.resource_type != other.resource_type:
            return False
        if self.created_at != other.created_at:
            return False
        return True


class _ArgumentNamespaceMock(NamedTuple):
    test_type: str
    test_suite: str = "not-specified"
    disk_size: int = 0
    disk_blocksize: int = 0


def test_cleanup():
    _now = datetime.now()
    to_delete_go_test_acceptance_common = [
        _Resource(
            name='acceptance-test-snapshot-acceptance-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=25),
        ),
        _Resource(
            name='acceptance-test-disk-acceptance-100000',
            resource_type='disk',
            created_at=_now - timedelta(days=5),
        ),
        _Resource(
            name='acceptance-test-image-acceptance-120384710234',
            resource_type='image',
            created_at=_now - timedelta(days=2),
        ),
    ]
    to_delete_go_test_acceptance_4gib_4kib = [
        *to_delete_go_test_acceptance_common,
        _Resource(
            name='acceptance-test-disk-acceptance-4gib-4kib-123124134',
            resource_type='disk',
            created_at=_now - timedelta(hours=28)
        ),
        _Resource(
            name='acceptance-test-image-acceptance-4gib-4kib-120849534',
            resource_type='image',
            created_at=_now - timedelta(hours=36),
        ),
        _Resource(
            name='acceptance-test-snapshot-acceptance-4gib-4kib-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=39),
        ),
    ]
    to_delete_go_test_acceptance_8tib_2kib = [
        *to_delete_go_test_acceptance_common,
        _Resource(
            name='acceptance-test-disk-acceptance-8tib-2kib-1823123123',
            resource_type='disk',
            created_at=_now - timedelta(hours=34)
        ),
        _Resource(
            name='acceptance-test-image-acceptance-8tib-2kib-10',
            resource_type='image',
            created_at=_now - timedelta(days=367),
        ),
        _Resource(
            name='acceptance-test-snapshot-acceptance-8tib-2kib-19199199',
            resource_type='snapshot',
            created_at=_now - timedelta(days=3),
        ),
        _Resource(
            name='acceptance-test-snapshot-acceptance-8tib-2kib-19',
            resource_type='snapshot',
            created_at=_now - timedelta(days=2),
        ),
    ]
    to_delete_go_test_eternal_common = [
        _Resource(
            name='acceptance-test-disk-eternal-12391293',
            resource_type='disk',
            created_at=_now - timedelta(days=6),
        ),
        _Resource(
            name='acceptance-test-disk-eternal-1',
            resource_type='disk',
            created_at=_now - timedelta(days=21),
        ),
        _Resource(
            name='acceptance-test-image-eternal-123123213',
            resource_type='image',
            created_at=_now - timedelta(days=7),
        ),
        _Resource(
            name='acceptance-test-snapshot-eternal-1707897850',
            resource_type='snapshot',
            created_at=_now - timedelta(days=11),
        ),
    ]
    to_delete_go_test_eternal_8tib_16gib = [
        *to_delete_go_test_eternal_common,
        _Resource(
            name='acceptance-test-disk-eternal-8tib-16gib-12391293',
            resource_type='disk',
            created_at=_now - timedelta(days=10),
        ),
        _Resource(
            name='acceptance-test-snapshot-eternal-8tib-16gib-1707897850',
            resource_type='snapshot',
            created_at=_now - timedelta(days=22),
        ),
        _Resource(
            name='acceptance-test-image-eternal-8tib-16gib-123123213',
            resource_type='image',
            created_at=_now - timedelta(days=12),
        ),
    ]
    to_delete_go_test_eternal_256gib_2mib = [
        *to_delete_go_test_eternal_common,
        _Resource(
            name='acceptance-test-disk-eternal-256gib-2mib-12391293',
            resource_type='disk',
            created_at=_now - timedelta(days=19),
        ),
        _Resource(
            name='acceptance-test-snapshot-eternal-256gib-2mib-1707897850',
            resource_type='snapshot',
            created_at=_now - timedelta(days=14),
        ),
        _Resource(
            name='acceptance-test-image-eternal-256gib-2mib-123123213',
            resource_type='image',
            created_at=_now - timedelta(days=15),
        ),
    ]
    to_delete_test_acceptance_small = [
        _Resource(
            name='acceptance-test-acceptance-small-2412341243',
            resource_type='instance',
            created_at=_now - timedelta(hours=38),
        ),
        _Resource(
            name='acceptance-test-acceptance-small-349856485',
            resource_type='disk',
            created_at=_now - timedelta(days=18),
        ),
    ]
    to_delete_test_acceptance_medium = [
        _Resource(
            name='acceptance-test-acceptance-medium-2412341243',
            resource_type='disk',
            created_at=_now - timedelta(days=8),
        ),
        _Resource(
            name='acceptance-test-acceptance-medium-23943849',
            resource_type='instance',
            created_at=_now - timedelta(hours=31)
        ),
        _Resource(
            name='acceptance-test-acceptance-medium-212',
            resource_type='disk',
            created_at=_now - timedelta(days=77),
        ),
    ]
    to_delete_test_acceptance_big = [
        _Resource(
            name='acceptance-test-acceptance-big-3745438',
            resource_type='disk',
            created_at=_now - timedelta(days=32),
        ),
        _Resource(
            name='acceptance-test-acceptance-big-2412341243',
            resource_type='instance',
            created_at=_now - timedelta(days=13),
        ),
    ]
    to_delete_test_acceptance_enormous = [
        _Resource(
            name='acceptance-test-acceptance-enormous-2412341243',
            resource_type='disk',
            created_at=_now - timedelta(days=9),
        ),
    ]
    to_delete_test_acceptance_default = [
        _Resource(
            name='acceptance-test-acceptance-enormous-2412341243',
            resource_type='instance',
            created_at=_now - timedelta(days=14),
        )
    ]
    to_delete_test_eternal_1tib_4kib = [
        _Resource(
            name='acceptance-test-eternal-1tib-4kib-1703757663',
            resource_type='disk',
            created_at=_now - timedelta(days=104),
        ),
        _Resource(
            name='acceptance-test-eternal-12931239',
            resource_type='instance',
            created_at=_now - timedelta(hours=31),
        ),
    ]
    to_delete_test_eternal_8gib_8kib = [
        _Resource(
            name='acceptance-test-eternal-1tib-4kib-1703757663',
            resource_type='disk',
            created_at=_now - timedelta(days=104),
        ),
        _Resource(
            name='acceptance-test-eternal-12931239',
            resource_type='instance',
            created_at=_now - timedelta(hours=27),
        ),
    ]
    to_delete_test_sync_8gib_4mib = [
        _Resource(
            name='acceptance-test-sync-1231312342',
            resource_type='instance',
            created_at=_now - timedelta(hours=35),
        ),
        _Resource(
            name='acceptance-test-sync-8gib-4mib-1703764831',
            resource_type='disk',
            created_at=_now - timedelta(days=7),
        ),
        _Resource(
            name='acceptance-test-sync-8gib-4mib-1706616105-from-snapshot',
            resource_type='disk',
            created_at=_now - timedelta(days=8),
        ),
        _Resource(
            name='sync-acceptance-test-snapshot-24-02-13-12-01-35',
            resource_type='snapshot',
            created_at=_now - timedelta(days=29),
        )
    ]
    not_deleted = [
        _Resource(
            name='acceptance-test-snapshot-acceptance-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=3),
        ),
        _Resource(
            name='acceptance-test-disk-acceptance-100000',
            resource_type='disk',
            created_at=_now - timedelta(hours=2),
        ),
        _Resource(
            name='acceptance-test-image-acceptance-120384710234',
            resource_type='image',
            created_at=_now,
        ),
        _Resource(
            name='acceptance-test-disk-acceptance-8tib-2kib-1823123123',
            resource_type='disk',
            created_at=_now,
        ),
        _Resource(
            name='acceptance-test-image-acceptance-8tib-2kib-10',
            resource_type='image',
            created_at=_now,
        ),
        _Resource(
            name='acceptance-test-snapshot-acceptance-8tib-2kib-19199199',
            resource_type='snapshot',
            created_at=_now,
        ),
        _Resource(
            name='acceptance-test-snapshot-acceptance-8tib-2kib-19',
            resource_type='snapshot',
            created_at=_now,
        ),
        _Resource(
            name='acceptance-test-disk-acceptance-8tib-8tib-123124134',
            resource_type='disk',
            created_at=_now - timedelta(hours=28)
        ),
        _Resource(
            name='acceptance-test-image-acceptance-8tib-8tib-120849534',
            resource_type='image',
            created_at=_now - timedelta(hours=36),
        ),
        _Resource(
            name='acceptance-test-snapshot-acceptance-8tib-8tib-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=1),
        ),
        _Resource(
            name='acceptance-test-acceptance-medium-2412341243',
            resource_type='disk',
            created_at=_now - timedelta(hours=3),
        ),
        _Resource(
            name='acceptance-test-acceptance-medium-23943849',
            resource_type='instance',
            created_at=_now - timedelta(hours=5)
        ),
        _Resource(
            name='acceptance-test-acceptance-medium-212',
            resource_type='disk',
            created_at=_now - timedelta(hours=2),
        ),
    ]
    ycp_mock = _YcpMock(
        [
            *{
                *to_delete_go_test_acceptance_4gib_4kib,
                *to_delete_go_test_acceptance_8tib_2kib,
                *to_delete_go_test_eternal_8tib_16gib,
                *to_delete_go_test_eternal_256gib_2mib,
                *to_delete_test_acceptance_small,
                *to_delete_test_acceptance_medium,
                *to_delete_test_acceptance_big,
                *to_delete_test_acceptance_enormous,
                *to_delete_test_acceptance_default,
                *to_delete_test_eternal_1tib_4kib,
                *to_delete_test_eternal_8gib_8kib,
                *to_delete_test_sync_8gib_4mib,
                *not_deleted,
            }
        ],
        RuntimeError,
    )
    go_test_cleanup_fixture = [
        (
            ('acceptance', 4, 4096, timedelta(days=1)),
            to_delete_go_test_acceptance_4gib_4kib,
        ),
        (
            ('acceptance', 8 * 1024, 2048, timedelta(days=1)),
            to_delete_go_test_acceptance_8tib_2kib,
        ),
        (
            ('eternal', 8 * 1024, 16 * 1024 ** 3, timedelta(days=5)),
            to_delete_go_test_eternal_8tib_16gib,
        ),
        (
            ('eternal', 256, 2 * 1024 ** 2, timedelta(days=5)),
            to_delete_go_test_eternal_256gib_2mib,
        ),
    ]
    for [cleanup_args, expected_to_delete_data] in go_test_cleanup_fixture:
        with ycp_mock.restoring_recorded_deletion():
            # noinspection PyTypeChecker
            cleanup_previous_acceptance_tests_results(
                ycp_mock, *cleanup_args,
            )
            assert set(
                ycp_mock.recorded_deletions,
            ) == set(
                expected_to_delete_data,
            ), f"Failed cleanup for args {cleanup_args}"
    test_cleanup_fixture = [
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='small',
            ),
            to_delete_test_acceptance_small,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='medium',
            ),
            to_delete_test_acceptance_medium,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='big',
            ),
            to_delete_test_acceptance_big,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='enormous',
            ),
            to_delete_test_acceptance_enormous,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='default',
            ),
            to_delete_test_acceptance_default,

        ),
        (
            EternalTestCleaner,
            _ArgumentNamespaceMock(
                test_type='eternal',
                disk_size=1024,
                disk_blocksize=4096,
            ),
            to_delete_test_eternal_1tib_4kib,

        ),
        (
            EternalTestCleaner,
            _ArgumentNamespaceMock(
                test_type='eternal',
                disk_size=8,
                disk_blocksize=8 * 1024,
            ),
            to_delete_test_eternal_8gib_8kib,

        ),
        (
            SyncTestCleaner,
            _ArgumentNamespaceMock(
                test_type='sync',
                disk_size=8,
                disk_blocksize=4 * 1024 ** 2,
            ),
            to_delete_test_sync_8gib_4mib,
        ),
    ]
    for [
        cleaner_type,
        cleanup_args,
        expected_to_delete_data,
    ] in test_cleanup_fixture:
        with ycp_mock.restoring_recorded_deletion():
            cleaner_type(ycp_mock, cleanup_args).cleanup()
            assert set(
                ycp_mock.recorded_deletions,
            ) == set(
                expected_to_delete_data,
            ), f"Failed cleanup for args {cleanup_args}"
