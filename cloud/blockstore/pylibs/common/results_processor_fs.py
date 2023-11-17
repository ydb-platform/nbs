import json
import os


class ResultsProcessorFsBase:

    def __init__(self, service, test_suite, cluster, date, results_path):
        self._service = service
        self._test_suite = test_suite
        self._cluster = cluster
        self._date = date
        self._results_path = results_path

        os.makedirs(results_path, exist_ok=True)
        os.makedirs(os.path.join(results_path, cluster), exist_ok=True)
        os.makedirs(os.path.join(results_path, cluster, service), exist_ok=True)
        os.makedirs(os.path.join(results_path, cluster, service, test_suite), exist_ok=True)
        os.makedirs(os.path.join(results_path, cluster, service, test_suite, date), exist_ok=True)

        self._path = os.path.join(results_path, cluster, service, test_suite, date)

    def publish_test_report_base(
        self,
        compute_node: str,
        id: str,
        disk_size: int,
        disk_type: str,
        disk_bs: int,
        extra_params: dict,
        test_case_name: str,
        error: any = None
    ) -> None:

        report = {
            'date': self._date,
            'compute_node': compute_node,
            'id': id,
            'size': disk_size,
            'type': disk_type,
            'disk_bs': disk_bs,
        }
        report.update(extra_params)

        if error is None:
            report['status'] = 'ok'
        else:
            report['status'] = 'fail'
            report['error_message'] = str(error)

        file_path = os.path.join(self._path, test_case_name + ".json")

        reports = []
        if os.path.isfile(file_path):
            with open(file_path, 'r') as f:
                reports = json.load(f)

        reports += [report]
        with open(file_path, 'w') as f:
            json.dump(reports, f)
