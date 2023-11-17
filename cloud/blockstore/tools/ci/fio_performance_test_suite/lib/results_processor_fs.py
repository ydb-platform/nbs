from typing import Set

from cloud.blockstore.pylibs.common.helpers import get_clat_mean_from_fio_report
from cloud.blockstore.pylibs.common.results_processor_fs import ResultsProcessorFsBase

from .test_cases import TestCase


class ResultsProcessorFs(ResultsProcessorFsBase):

    def __init__(self, service, test_suite, cluster, date, results_path):
        super(ResultsProcessorFs, self).__init__(
            service,
            test_suite,
            cluster,
            date,
            results_path)

    def fetch_completed_test_cases(*args) -> Set[TestCase]:
        return set()

    def publish_test_report(
        self,
        compute_node: str,
        id: str,
        test_case: TestCase,
        fio_report: dict,
        exception: any = None
    ) -> None:

        report = {
            'rw': test_case.rw,
            'bs': test_case.bs,
            'iodepth': test_case.iodepth,
            'rw_mix_read': test_case.rw_mix_read,
        }

        if exception is None:
            job = fio_report['jobs'][0]
            r = job['read']
            w = job['write']

            report['result'] = {
                'read_iops': r['iops'],
                'read_bw': r['bw'],
                'read_clat_mean': get_clat_mean_from_fio_report(r),
                'write_iops': w['iops'],
                'write_bw': w['bw'],
                'write_clat_mean': get_clat_mean_from_fio_report(w),
            }

        test_case_name = f'{test_case.size}GB_{test_case.type}_{(test_case.device_bs / 1024):g}KB_' \
            + f'{(test_case.bs / 1024):g}KB_{test_case.rw}_{test_case.iodepth}_{test_case.rw_mix_read}_result'

        self.publish_test_report_base(
            compute_node,
            id,
            test_case.size,
            test_case.type,
            test_case.device_bs,
            report,
            test_case_name,
            exception)

    def announce_test_run(*args) -> None:
        pass
