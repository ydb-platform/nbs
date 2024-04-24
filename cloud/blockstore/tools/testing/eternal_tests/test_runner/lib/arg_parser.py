import argparse
import sys

from .test_configs import get_test_case_description

from cloud.blockstore.pylibs import common


class ParseHelper:
    remaining_args: [str]
    commands: [str]

    def __init__(self, commands: [str]):
        self.remaining_args = sys.argv[1:]
        self.commands = commands
        self.args = argparse.Namespace()

    def parse_load_options(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

        parser.add_argument(
            '--refill',
            action='store_true',
            help='specify, if you want to refill disk before start load'
        )

        parser.add_argument(
            '--write-rate',
            type=int,
            help='specify write rate'
        )

        parser.add_argument(
            '--scp-binary',
            action='store_true',
            help='specify, if you want to scp new eternal-load binary'
        )

        parser.add_argument(
            '--force-rerun',
            action='store_true',
            help='specify, if you want to force loadtest rerun even if it\'s'
            ' already running'
        )

        args = parser.parse_args(self.remaining_args)
        self.args = argparse.Namespace(**vars(self.args), **vars(args))

    def parse_run_test_options(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

        parser.add_argument(
            '--placement-group-name',
            type=str,
            help='specify placement policy group name'
        )

        parser.add_argument(
            '--compute-node',
            type=str,
            default=None,
            help='specify compute node for instance creation'
        )

        parser.add_argument(
            '--host-group',
            type=str,
            default=None,
            help='specify host group for instance creation'
        )

        args = parser.parse_args(self.remaining_args)
        self.args = argparse.Namespace(**vars(self.args), **vars(args))

    def parse_command(self):
        parser = argparse.ArgumentParser(description='Examples:' +
                                                     '\n\t./yc-nbs-run-eternal-load-tests setup-test ' +
                                                     '--cluster <cluster> ' +
                                                     '--cluster-config-path <config-path> ' +
                                                     '--test-case eternal-640gb-verify-checkpoint ' +
                                                     '--no-generate-ycp-config ' +
                                                     '--zone-id a',
                                         formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument('command', choices=self.commands)

        verbose_quite_group = parser.add_mutually_exclusive_group()
        verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
        verbose_quite_group.add_argument('-q', '--quite', action='store_true')

        common.add_common_parser_arguments(parser)

        parser.add_argument('--compress', action='store_true')

        parser.add_argument(
            '--test-case',
            type=str,
            required=True,
            help='specify the test case or <all>, if you want to manage all test-cases' + get_test_case_description())

        parser.add_argument(
            '--file-path',
            type=str,
            help='specify file path to run load')
        parser.add_argument(
            '--zone-id',
            type=str,
            help='specify zone id'
        )

        self.args, self.remaining_args = parser.parse_known_args(self.remaining_args)

    def get_args(self):
        return self.args
