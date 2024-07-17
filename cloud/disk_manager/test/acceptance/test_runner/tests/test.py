import contextlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
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
    disk_type: str = ""
    disk_size: int = 0
    disk_blocksize: int = 0


def test_cleanup():
    _now = datetime.now(timezone.utc)
    should_delete_inner_test_acceptance_common = [
        _Resource(
            name='acc-snapshot-acceptance-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=25),
        ),
        _Resource(
            name='acc-disk-acceptance-100000',
            resource_type='disk',
            created_at=_now - timedelta(days=5),
        ),
        _Resource(
            name='acc-image-acceptance-120384710234',
            resource_type='image',
            created_at=_now - timedelta(days=2),
        ),
    ]
    should_delete_inner_test_acceptance_4gib_4kib = [
        *should_delete_inner_test_acceptance_common,
        _Resource(
            name='acc-disk-acceptance-network-ssd-4gib-4kib-123124134',
            resource_type='disk',
            created_at=_now - timedelta(hours=28)
        ),
        _Resource(
            name='acc-image-acceptance-network-ssd-4gib-4kib-120849534',
            resource_type='image',
            created_at=_now - timedelta(hours=36),
        ),
        _Resource(
            name='acc-snapshot-acceptance-network-ssd-4gib-4kib-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=39),
        ),
    ]
    should_delete_inner_test_acceptance_8tib_2kib = [
        *should_delete_inner_test_acceptance_common,
        _Resource(
            name='acc-disk-acceptance-network-ssd-8tib-2kib-1823123123',
            resource_type='disk',
            created_at=_now - timedelta(hours=34)
        ),
        _Resource(
            name='acc-image-acceptance-network-ssd-8tib-2kib-10',
            resource_type='image',
            created_at=_now - timedelta(days=367),
        ),
        _Resource(
            name='acc-snapshot-acceptance-network-ssd-8tib-2kib-19199199',
            resource_type='snapshot',
            created_at=_now - timedelta(days=3),
        ),
        _Resource(
            name='acc-snapshot-acceptance-network-ssd-8tib-2kib-19',
            resource_type='snapshot',
            created_at=_now - timedelta(days=2),
        ),
    ]
    should_delete_inner_test_acceptance_nonrepl_93gib_8kib = [
        *should_delete_inner_test_acceptance_common,
        _Resource(
            name='acc-disk-acceptance-ssd-nonrepl-93gib-8kib-1823123123',
            resource_type='disk',
            created_at=_now - timedelta(hours=34)
        ),
        _Resource(
            name='acc-image-acceptance-ssd-nonrepl-93gib-8kib-10',
            resource_type='image',
            created_at=_now - timedelta(days=367),
        ),
        _Resource(
            name='acc-snapshot-acceptance-ssd-nonrepl-93gib-8kib-19199199',
            resource_type='snapshot',
            created_at=_now - timedelta(days=3),
        ),
        _Resource(
            name='acc-snapshot-acceptance-ssd-nonrepl-93gib-8kib-19',
            resource_type='snapshot',
            created_at=_now - timedelta(days=2),
        ),
    ]
    should_delete_inner_test_eternal_common = [
        _Resource(
            name='acc-disk-eternal-12391293',
            resource_type='disk',
            created_at=_now - timedelta(days=6),
        ),
        _Resource(
            name='acc-disk-eternal-1',
            resource_type='disk',
            created_at=_now - timedelta(days=21),
        ),
        _Resource(
            name='acc-image-eternal-123123213',
            resource_type='image',
            created_at=_now - timedelta(days=7),
        ),
        _Resource(
            name='acc-snapshot-eternal-1707897850',
            resource_type='snapshot',
            created_at=_now - timedelta(days=11),
        ),
    ]
    should_delete_inner_test_eternal_8tib_16gib = [
        *should_delete_inner_test_eternal_common,
        _Resource(
            name='acc-disk-eternal-network-ssd-8tib-16gib-12391293',
            resource_type='disk',
            created_at=_now - timedelta(days=10),
        ),
        _Resource(
            name='acc-snapshot-eternal-network-ssd-8tib-16gib-1707897850',
            resource_type='snapshot',
            created_at=_now - timedelta(days=22),
        ),
        _Resource(
            name='acc-image-eternal-network-ssd-8tib-16gib-123123213',
            resource_type='image',
            created_at=_now - timedelta(days=12),
        ),
    ]
    should_delete_inner_test_eternal_256gib_2mib = [
        *should_delete_inner_test_eternal_common,
        _Resource(
            name='acc-disk-eternal-network-ssd-256gib-2mib-12391293',
            resource_type='disk',
            created_at=_now - timedelta(days=19),
        ),
        _Resource(
            name='acc-snapshot-eternal-network-ssd-256gib-2mib-1707897850',
            resource_type='snapshot',
            created_at=_now - timedelta(days=14),
        ),
        _Resource(
            name='acc-image-eternal-network-ssd-256gib-2mib-123123213',
            resource_type='image',
            created_at=_now - timedelta(days=15),
        ),
    ]
    should_delete_inner_test_eternal_nonrepl_372gib_8kib = [
        *should_delete_inner_test_eternal_common,
        _Resource(
            name='acc-disk-eternal-ssd-nonrepl-372gib-8kib-12391293',
            resource_type='disk',
            created_at=_now - timedelta(days=19),
        ),
        _Resource(
            name='acc-snapshot-eternal-ssd-nonrepl-372gib-8kib-1707897850',
            resource_type='snapshot',
            created_at=_now - timedelta(days=14),
        ),
        _Resource(
            name='acc-image-eternal-ssd-nonrepl-372gib-8kib-123123213',
            resource_type='image',
            created_at=_now - timedelta(days=15),
        ),
    ]
    should_delete_test_acceptance_small = [
        _Resource(
            name='acc-acceptance-small-2412341243',
            resource_type='instance',
            created_at=_now - timedelta(hours=38),
        ),
        _Resource(
            name='acc-acceptance-small-349856485',
            resource_type='disk',
            created_at=_now - timedelta(days=18),
        ),
    ]
    should_delete_test_acceptance_medium = [
        _Resource(
            name='acc-acceptance-medium-2412341243',
            resource_type='disk',
            created_at=_now - timedelta(days=8),
        ),
        _Resource(
            name='acc-acceptance-medium-23943849',
            resource_type='instance',
            created_at=_now - timedelta(hours=31)
        ),
        _Resource(
            name='acc-acceptance-medium-212',
            resource_type='disk',
            created_at=_now - timedelta(days=77),
        ),
    ]
    should_delete_test_acceptance_big = [
        _Resource(
            name='acc-acceptance-big-3745438',
            resource_type='disk',
            created_at=_now - timedelta(days=32),
        ),
        _Resource(
            name='acc-acceptance-big-2412341243',
            resource_type='instance',
            created_at=_now - timedelta(days=13),
        ),
    ]
    should_delete_test_acceptance_enormous = [
        _Resource(
            name='acc-acceptance-enormous-2412341243',
            resource_type='disk',
            created_at=_now - timedelta(days=9),
        ),
    ]
    should_delete_test_acceptance_drbased = [
        _Resource(
            name='acc-acceptance-drbased-2412341243',
            resource_type='disk',
            created_at=_now - timedelta(days=9),
        ),
    ]
    should_delete_test_acceptance_default = [
        _Resource(
            name='acc-acceptance-default-2412341243',
            resource_type='instance',
            created_at=_now - timedelta(days=14),
        )
    ]
    should_delete_inner_test_eternal_common = [
        _Resource(
            name='acc-eternal-12931239',
            resource_type='instance',
            created_at=_now - timedelta(hours=31),
        ),
    ]
    should_delete_test_eternal_1tib_4kib = [
        _Resource(
            name='acc-eternal-network-ssd-1tib-4kib-1703757663',
            resource_type='disk',
            created_at=_now - timedelta(days=104),
        ),
        *should_delete_inner_test_eternal_common,
    ]
    should_delete_test_eternal_8gib_8kib = [
        _Resource(
            name='acc-eternal-network-ssd-8gib-8kib-1703757663',
            resource_type='disk',
            created_at=_now - timedelta(days=104),
        ),
        *should_delete_inner_test_eternal_common,
    ]
    should_delete_test_eternal_nonrepl_93gib_4kib = [
        _Resource(
            name='acc-eternal-ssd-nonrepl-93gib-4kib-1703757663',
            resource_type='disk',
            created_at=_now - timedelta(days=104),
        ),
        *should_delete_inner_test_eternal_common,
    ]
    should_delete_test_sync_8gib_4mib = [
        _Resource(
            name='acc-sync-1231312342',
            resource_type='instance',
            created_at=_now - timedelta(hours=35),
        ),
        _Resource(
            name='acc-sync-network-ssd-8gib-4mib-1703764831',
            resource_type='disk',
            created_at=_now - timedelta(days=7),
        ),
        _Resource(
            name='acc-sync-network-ssd-8gib-4mib-1706616105-from-snapshot',
            resource_type='disk',
            created_at=_now - timedelta(days=8),
        ),
        _Resource(
            name='sync-acc-snapshot-24-02-13-12-01-35',
            resource_type='snapshot',
            created_at=_now - timedelta(days=29),
        )
    ]
    should_not_delete = [
        _Resource(
            name='acc-snapshot-acceptance-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=3),
        ),
        _Resource(
            name='acc-disk-acceptance-100000',
            resource_type='disk',
            created_at=_now - timedelta(hours=2),
        ),
        _Resource(
            name='acc-image-acceptance-120384710234',
            resource_type='image',
            created_at=_now,
        ),
        _Resource(
            name='acc-disk-acceptance-network-ssd-8tib-2kib-1823123123',
            resource_type='disk',
            created_at=_now,
        ),
        _Resource(
            name='acc-image-acceptance-network-ssd-8tib-2kib-10',
            resource_type='image',
            created_at=_now,
        ),
        _Resource(
            name='acc-snapshot-acceptance-network-ssd-8tib-2kib-19199199',
            resource_type='snapshot',
            created_at=_now,
        ),
        _Resource(
            name='acc-snapshot-acceptance-network-ssd-8tib-2kib-19',
            resource_type='snapshot',
            created_at=_now,
        ),
        _Resource(
            name='acc-disk-acceptance-network-ssd-8tib-8tib-123124134',
            resource_type='disk',
            created_at=_now - timedelta(hours=28)
        ),
        _Resource(
            name='acc-image-acceptance-network-ssd-8tib-8tib-120849534',
            resource_type='image',
            created_at=_now - timedelta(hours=36),
        ),
        _Resource(
            name='acc-snapshot-acceptance-network-ssd-8tib-8tib-12931283',
            resource_type='snapshot',
            created_at=_now - timedelta(hours=1),
        ),
        _Resource(
            name='acc-acceptance-medium-2412341243',
            resource_type='disk',
            created_at=_now - timedelta(hours=3),
        ),
        _Resource(
            name='acc-acceptance-medium-23943849',
            resource_type='instance',
            created_at=_now - timedelta(hours=5)
        ),
        _Resource(
            name='acc-acceptance-medium-212',
            resource_type='disk',
            created_at=_now - timedelta(hours=2),
        ),
        _Resource(
            name='acc-eternal-network-ssd-93gib-4kib-1703757663',
            resource_type='disk',
            created_at=_now - timedelta(days=104),
        ),
    ]
    ycp_mock = _YcpMock(
        [
            *{
                *should_delete_inner_test_acceptance_4gib_4kib,
                *should_delete_inner_test_acceptance_8tib_2kib,
                *should_delete_inner_test_acceptance_nonrepl_93gib_8kib,
                *should_delete_inner_test_eternal_8tib_16gib,
                *should_delete_inner_test_eternal_256gib_2mib,
                *should_delete_inner_test_eternal_nonrepl_372gib_8kib,
                *should_delete_test_acceptance_small,
                *should_delete_test_acceptance_medium,
                *should_delete_test_acceptance_big,
                *should_delete_test_acceptance_enormous,
                *should_delete_test_acceptance_drbased,
                *should_delete_test_acceptance_default,
                *should_delete_test_eternal_1tib_4kib,
                *should_delete_test_eternal_8gib_8kib,
                *should_delete_test_eternal_nonrepl_93gib_4kib,
                *should_delete_test_sync_8gib_4mib,
                *should_not_delete,
            }
        ],
        RuntimeError,
    )
    gib = 1024 ** 3
    inner_test_cleanup_fixture = [
        (
            ('acceptance', 'network-ssd', 4 * gib, 4096, timedelta(days=1)),
            should_delete_inner_test_acceptance_4gib_4kib,
        ),
        (
            ('acceptance', 'network-ssd', 8 * 1024 * gib, 2048, timedelta(days=1)),
            should_delete_inner_test_acceptance_8tib_2kib,
        ),
        (
            ('acceptance', 'network-ssd-nonreplicated', 93 * gib, 8192, timedelta(days=1)),
            should_delete_inner_test_acceptance_nonrepl_93gib_8kib,
        ),
        (
            ('eternal', 'network-ssd', 8 * 1024 * gib, 16 * 1024 ** 3, timedelta(days=5)),
            should_delete_inner_test_eternal_8tib_16gib,
        ),
        (
            ('eternal', 'network-ssd', 256 * gib, 2 * 1024 ** 2, timedelta(days=5)),
            should_delete_inner_test_eternal_256gib_2mib,
        ),
        (
            ('eternal', 'network-ssd-nonreplicated', 372 * gib, 8192, timedelta(days=5)),
            should_delete_inner_test_eternal_nonrepl_372gib_8kib,
        ),
    ]
    for [
        cleanup_args,
        expected_should_delete_data,
    ] in inner_test_cleanup_fixture:
        with ycp_mock.restoring_recorded_deletion():
            # noinspection PyTypeChecker
            cleanup_previous_acceptance_tests_results(
                ycp_mock, *cleanup_args,
            )
            assert set(
                ycp_mock.recorded_deletions,
            ) == set(
                expected_should_delete_data,
            ), f"Failed cleanup for args {cleanup_args}"
    test_cleanup_fixture = [
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='small',
            ),
            should_delete_test_acceptance_small,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='medium',
            ),
            should_delete_test_acceptance_medium,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='big',
            ),
            should_delete_test_acceptance_big,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='enormous',
            ),
            should_delete_test_acceptance_enormous,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='drbased',
            ),
            should_delete_test_acceptance_drbased,

        ),
        (
            AcceptanceTestCleaner,
            _ArgumentNamespaceMock(
                test_type='acceptance',
                test_suite='default',
            ),
            should_delete_test_acceptance_default,

        ),
        (
            EternalTestCleaner,
            _ArgumentNamespaceMock(
                test_type='eternal',
                disk_type='network-ssd',
                disk_size=1024,
                disk_blocksize=4096,
            ),
            should_delete_test_eternal_1tib_4kib,

        ),
        (
            EternalTestCleaner,
            _ArgumentNamespaceMock(
                test_type='eternal',
                disk_type='network-ssd',
                disk_size=8,
                disk_blocksize=8 * 1024,
            ),
            should_delete_test_eternal_8gib_8kib,

        ),
        (
            EternalTestCleaner,
            _ArgumentNamespaceMock(
                test_type='eternal',
                disk_type='network-ssd-nonreplicated',
                disk_size=93,
                disk_blocksize=4 * 1024,
            ),
            should_delete_test_eternal_nonrepl_93gib_4kib,

        ),
        (
            SyncTestCleaner,
            _ArgumentNamespaceMock(
                test_type='sync',
                disk_type='network-ssd',
                disk_size=8,
                disk_blocksize=4 * 1024 ** 2,
            ),
            should_delete_test_sync_8gib_4mib,
        ),
    ]
    for [
        cleaner_type,
        cleanup_args,
        expected_should_delete_data,
    ] in test_cleanup_fixture:
        with ycp_mock.restoring_recorded_deletion():
            cleaner_type(ycp_mock, cleanup_args).cleanup()
            assert set(
                ycp_mock.recorded_deletions,
            ) == set(
                expected_should_delete_data,
            ), f"Failed cleanup for args {cleanup_args}"
