"""Command-line interface."""
import sys
import os
import traceback
import argparse

from dbt.main import (
    DBTArgumentParser,
    _add_version_check,
    _build_base_subparser,
    initialize_config_values,
    adapter_management,
    run_from_args
)
from dbt.utils import ExitCodes
from dbt.exceptions import RuntimeException
from dbt_rpc.task.server import RPCServerTask

from dbt.profiler import profiler
from dbt.logger import GLOBAL_LOGGER as logger, log_manager


class RPCArgumentParser(DBTArgumentParser):
    def exit(self, status=0, message=None):
        if status == 0:
            return
        else:
            raise TypeError(message)


def _build_rpc_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'serve',
        parents=[base_subparser],
        help='''
        Start a json-rpc server
        ''',
    )
    sub.add_argument(
        '--host',
        default='0.0.0.0',
        help='''
        Specify the host to listen on for the rpc server.
        ''',
    )
    sub.add_argument(
        '--port',
        default=8580,
        type=int,
        help='''
        Specify the port number for the rpc server.
        ''',
    )
    sub.set_defaults(cls=RPCServerTask, which='rpc', rpc_method=None)
    # the rpc task does a 'compile', so we need these attributes to exist, but
    # we don't want users to be allowed to set them.
    sub.set_defaults(models=None, exclude=None)
    sub.add_argument(
        '--threads',
        type=int,
        required=False,
        help='''
        Specify number of threads to use while executing models. Overrides
        settings in profiles.yml.
        '''
    )
    _add_version_check(sub)
    return sub


def build_parser(cls=DBTArgumentParser):
    p = cls(
        prog='dbt-rpc',
        description='''
        An ELT tool for managing your SQL transformations and data models.
        For more documentation on these commands, visit: docs.getdbt.com
        ''',
        epilog='''
        Specify one of these sub-commands and you can find more help from
        there.
        '''
    )

    p.add_argument(
        '--version',
        action='dbtversion',
        help='''
        Show version information
        ''')

    p.add_argument(
        '-r',
        '--record-timing-info',
        default=None,
        type=str,
        help='''
        When this option is passed, dbt will output low-level timing stats to
        the specified file. Example: `--record-timing-info output.profile`
        '''
    )

    p.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='''
        Display debug logging during dbt execution. Useful for debugging and
        making bug reports.
        '''
    )

    p.add_argument(
        '--log-format',
        choices=['text', 'json', 'default'],
        default='default',
        help='''Specify the log format, overriding the command's default.'''
    )

    p.add_argument(
        '--no-write-json',
        action='store_false',
        dest='write_json',
        help='''
        If set, skip writing the manifest and run_results.json files to disk
        '''
    )
    colors_flag = p.add_mutually_exclusive_group()
    colors_flag.add_argument(
        '--use-colors',
        action='store_const',
        const=True,
        dest='use_colors',
        help='''
        Colorize the output DBT prints to the terminal. Output is colorized by
        default and may also be set in a profile or at the command line.
        Mutually exclusive with --no-use-colors
        '''
    )
    colors_flag.add_argument(
        '--no-use-colors',
        action='store_const',
        const=False,
        dest='use_colors',
        help='''
        Do not colorize the output DBT prints to the terminal. Output is
        colorized by default and may also be set in a profile or at the
        command line.
        Mutually exclusive with --use-colors
        '''
    )

    p.add_argument(
        '-S',
        '--strict',
        action='store_true',
        help='''
        Run schema validations at runtime. This will surface bugs in dbt, but
        may incur a performance penalty.
        '''
    )

    p.add_argument(
        '--warn-error',
        action='store_true',
        help='''
        If dbt would normally warn, instead raise an exception. Examples
        include --models that selects nothing, deprecations, configurations
        with no associated models, invalid test configurations, and missing
        sources/refs in tests.
        '''
    )

    p.add_optional_argument_inverse(
        '--partial-parse',
        enable_help='''
        Allow for partial parsing by looking for and writing to a pickle file
        in the target directory. This overrides the user configuration file.

        WARNING: This can result in unexpected behavior if you use env_var()!
        ''',
        disable_help='''
        Disallow partial parsing. This overrides the user configuration file.
        ''',
    )

    # if set, run dbt in single-threaded mode: thread count is ignored, and
    # calls go through `map` instead of the thread pool. This is useful for
    # getting performance information about aspects of dbt that normally run in
    # a thread, as the profiler ignores child threads. Users should really
    # never use this.
    p.add_argument(
        '--single-threaded',
        action='store_true',
        help=argparse.SUPPRESS,
    )

    # if set, extract all models and blocks with the jinja block extractor, and
    # verify that we don't fail anywhere the actual jinja parser passes. The
    # reverse (passing files that ends up failing jinja) is fine.
    # TODO remove?
    p.add_argument(
        '--test-new-parser',
        action='store_true',
        help=argparse.SUPPRESS
    )

    # if set, will use the tree-sitter-jinja2 parser and extractor instead of
    # jinja rendering when possible.
    p.add_argument(
        '--use-experimental-parser',
        action='store_true',
        help='''
        Uses an experimental parser to extract jinja values.
        '''
    )

    subs = p.add_subparsers(title="Available sub-commands")

    base_subparser = _build_base_subparser()
    _build_rpc_subparser(subs, base_subparser)
    return p


def parse_args(args):
    p = build_parser()

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    if hasattr(parsed, 'profiles_dir'):
        parsed.profiles_dir = os.path.abspath(parsed.profiles_dir)

    if getattr(parsed, 'project_dir', None) is not None:
        expanded_user = os.path.expanduser(parsed.project_dir)
        parsed.project_dir = os.path.abspath(expanded_user)

    if not hasattr(parsed, 'which'):
        # the user did not provide a valid subcommand. trigger the help message
        # and exit with a error
        p.print_help()
        p.exit(1)

    return parsed


def handle_and_check(args):
    with log_manager.applicationbound():
        parsed = parse_args(args)

        # we've parsed the args - we can now decide if we're debug or not
        if parsed.debug:
            log_manager.set_debug()

        profiler_enabled = False

        if parsed.record_timing_info:
            profiler_enabled = True

        with profiler(
            enable=profiler_enabled,
            outfile=parsed.record_timing_info
        ):

            initialize_config_values(parsed)

            with adapter_management():

                task, res = run_from_args(parsed)
                success = task.interpret_results(res)

            return res, success


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    with log_manager.applicationbound():
        try:
            results, succeeded = handle_and_check(args)
            if succeeded:
                exit_code = ExitCodes.Success.value
            else:
                exit_code = ExitCodes.ModelError.value

        except KeyboardInterrupt:
            logger.info("ctrl-c")
            exit_code = ExitCodes.UnhandledError.value

        # This can be thrown by eg. argparse
        except SystemExit as e:
            exit_code = e.code

        except BaseException as e:
            logger.warning("Encountered an error:")
            logger.warning(str(e))

            if log_manager.initialized:
                logger.debug(traceback.format_exc())
            elif not isinstance(e, RuntimeException):
                # if it did not come from dbt proper and the logger is not
                # initialized (so there's no safe path to log to), log the
                # stack trace at error level.
                logger.error(traceback.format_exc())
            exit_code = ExitCodes.UnhandledError.value

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
