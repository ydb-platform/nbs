import pytest
import logging
from dataclasses import dataclass
from .nebius_populate_vms import decide_scaling

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@dataclass
class ScalingTestCase:
    alive: int
    idle: int
    busy: int
    remove: int
    max_vms_to_create: int
    maximum_amount_of_vms_to_have: int
    extra_vms_if_needed: int
    expected_create: int
    expected_excess_idle: int
    expected_projected: int
    expect_exception: bool = False


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            ScalingTestCase(
                alive=0,
                idle=0,
                busy=0,
                remove=0,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=1,
                expected_excess_idle=0,
                expected_projected=1,
                expect_exception=False,
            ),
            id="no-vms-creates-one",
        ),
        pytest.param(
            ScalingTestCase(
                alive=1,
                idle=1,
                busy=0,
                remove=0,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=0,
                expected_projected=1,
                expect_exception=False,
            ),
            id="idle-vm-sufficient",
        ),
        pytest.param(
            ScalingTestCase(
                alive=1,
                idle=0,
                busy=1,
                remove=0,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=1,
                expected_excess_idle=0,
                expected_projected=2,
                expect_exception=False,
            ),
            id="one-busy-vm-create-one",
        ),
        pytest.param(
            ScalingTestCase(
                alive=1,
                idle=1,
                busy=0,
                remove=1,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=1,
                expected_excess_idle=0,
                expected_projected=1,
                expect_exception=False,
            ),
            id="idle-vm-removed-by-ttl",
        ),
        pytest.param(
            ScalingTestCase(
                alive=2,
                idle=0,
                busy=2,
                remove=0,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=1,
                expected_excess_idle=0,
                expected_projected=3,
                expect_exception=False,
            ),
            id="busy-vms-allow-extra",
        ),
        pytest.param(
            ScalingTestCase(
                alive=3,
                idle=2,
                busy=1,
                remove=0,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=1,
                expected_projected=2,
                expect_exception=False,
            ),
            id="too-many-idle-no-create",
        ),
        pytest.param(
            ScalingTestCase(
                alive=3,
                idle=0,
                busy=3,
                remove=3,
                max_vms_to_create=2,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=2,
                expected_create=2,
                expected_excess_idle=0,
                expected_projected=0,
                expect_exception=True,
            ),
            id="all-busy-vms-to-remove-expect-exception",
        ),
        pytest.param(
            ScalingTestCase(
                alive=4,
                idle=3,
                busy=1,
                remove=0,
                max_vms_to_create=2,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=1,
                expected_projected=3,
                expect_exception=False,
            ),
            id="excess-idle-no-create",
        ),
        pytest.param(
            ScalingTestCase(
                alive=4,
                idle=2,
                busy=2,
                remove=0,
                max_vms_to_create=2,
                maximum_amount_of_vms_to_have=4,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=0,
                expected_projected=4,
                expect_exception=False,
            ),
            id="projected-equals-cap-no-create",
        ),
        pytest.param(
            ScalingTestCase(
                alive=4,
                idle=2,
                busy=2,
                remove=1,
                max_vms_to_create=2,
                maximum_amount_of_vms_to_have=3,
                extra_vms_if_needed=1,
                expected_create=1,
                expected_excess_idle=0,
                expected_projected=4,
                expect_exception=True,
            ),
            id="projected-exceeds-cap-expect-exception",
        ),
        pytest.param(
            ScalingTestCase(
                alive=5,
                idle=0,
                busy=5,
                remove=0,
                max_vms_to_create=2,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=2,
                expected_create=0,
                expected_excess_idle=0,
                expected_projected=5,
                expect_exception=False,
            ),
            id="maxed-out-no-room-to-scale",
        ),
        pytest.param(
            ScalingTestCase(
                alive=5,
                idle=1,
                busy=4,
                remove=1,
                max_vms_to_create=2,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=2,
                expected_create=1,
                expected_excess_idle=0,
                expected_projected=5,
                expect_exception=False,
            ),
            id="remove-one-add-one-to-maintain-cap",
        ),
        pytest.param(
            ScalingTestCase(
                alive=2,
                idle=1,
                busy=1,
                remove=0,
                max_vms_to_create=4,
                maximum_amount_of_vms_to_have=6,
                extra_vms_if_needed=2,
                expected_create=2,
                expected_excess_idle=0,
                expected_projected=4,
                expect_exception=False,
            ),
            id="some-idle-some-busy-create-more-to-meet-threshold",
        ),
        pytest.param(
            ScalingTestCase(
                alive=2,
                idle=0,
                busy=2,
                remove=0,
                max_vms_to_create=4,
                maximum_amount_of_vms_to_have=6,
                extra_vms_if_needed=20,
                expected_create=2,
                expected_excess_idle=0,
                expected_projected=4,
                expect_exception=False,
            ),
            id="all-busy-big-extra",
        ),
        pytest.param(
            ScalingTestCase(
                alive=4,
                idle=0,
                busy=4,
                remove=0,
                max_vms_to_create=4,
                maximum_amount_of_vms_to_have=6,
                extra_vms_if_needed=20,
                expected_create=2,
                expected_excess_idle=0,
                expected_projected=6,
                expect_exception=False,
            ),
            id="all-busy-big-extra-step-2",
        ),
        pytest.param(
            ScalingTestCase(
                alive=5,
                idle=5,
                busy=0,
                remove=0,
                max_vms_to_create=2,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=3,
                expected_projected=2,
                expect_exception=False,
            ),
            id="remove-from-idle-create-to-maintain",
        ),
        pytest.param(
            ScalingTestCase(
                alive=5,
                idle=5,
                busy=0,
                remove=3,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=4,
                expected_projected=1,
                expect_exception=False,
            ),
            id="remove-vms-and-remove-excess-idle",
        ),
        pytest.param(
            ScalingTestCase(
                alive=5,
                idle=5,
                busy=0,
                remove=0,
                max_vms_to_create=-1,
                maximum_amount_of_vms_to_have=4,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=5,
                expected_projected=0,
                expect_exception=False,
            ),
            id="downscale-to-zero-idle-vms",
        ),
        pytest.param(
            ScalingTestCase(
                alive=5,
                idle=2,
                busy=3,
                remove=0,
                max_vms_to_create=-1,
                maximum_amount_of_vms_to_have=4,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=2,
                expected_projected=3,
                expect_exception=False,
            ),
            id="downscale-to-some-busy-vms",
        ),
        pytest.param(
            ScalingTestCase(
                alive=2,
                idle=1,
                busy=1,
                remove=0,
                max_vms_to_create=1,
                maximum_amount_of_vms_to_have=5,
                extra_vms_if_needed=1,
                expected_create=0,
                expected_excess_idle=0,
                expected_projected=2,
                expect_exception=False,
            ),
            id="check-if-we-wont-create-vms",
        ),
    ],
)
def test_decide_scaling(case):
    exception_type = None
    try:
        to_create, excess_idle, projected_vm_count = decide_scaling(
            case.alive,
            case.idle,
            case.busy,
            case.remove,
            case.max_vms_to_create,
            case.maximum_amount_of_vms_to_have,
            case.extra_vms_if_needed,
        )
    except ValueError as e:
        if case.expect_exception:
            assert type(e) is ValueError
        else:
            assert (
                not case.expect_exception
            ), f"Expected no exception, but got {exception_type}"
            raise

    if not case.expect_exception:
        assert (
            to_create == case.expected_create
        ), f"Expected expected_create={case.expected_create}, got {to_create}"
        assert (
            excess_idle == case.expected_excess_idle
        ), f"Expected expected_excess_idle={case.expected_excess_idle}, got {excess_idle}"
        assert (
            projected_vm_count == case.expected_projected
        ), f"Expected expected_projected={case.expected_projected}, got {projected_vm_count}"
