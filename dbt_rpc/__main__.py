from dbt.logger import GLOBAL_LOGGER as logger, log_cache_events, log_manager

import argparse
import os.path
import sys
import traceback
from contextlib import contextmanager

import dbt.version
from dbt.profiler import profiler
from dbt.adapters.factory import reset_adapters, cleanup_connections

import dbt.tracking

from dbt.utils import ExitCodes
from dbt.config.profile import DEFAULT_PROFILES_DIR, read_user_config
from dbt.exceptions import (
    RuntimeException,
    InternalException,
    NotImplementedException,
    FailedToConnectException
)
import dbt.flags as flags

from dbt_rpc.task.server import RPCServerTask


def initialize_tracking_from_flags():
    # NOTE: this is copied from dbt-core
    # Setting these used to be in UserConfig, but had to be moved here
    if flags.SEND_ANONYMOUS_USAGE_STATS:
        dbt.tracking.initialize_tracking(flags.PROFILES_DIR)
    else:
        dbt.tracking.do_not_track()


class DBTVersion(argparse.Action):
    """This is very very similar to the builtin argparse._Version action,
    except it just calls dbt.version.get_version_information().
    """

    def __init__(self,
                 option_strings,
                 version=None,
                 dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS,
                 help="show program's version number and exit"):
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        formatter = argparse.RawTextHelpFormatter(prog=parser.prog)
        formatter.add_text(dbt.version.get_version_information())
        parser.exit(message=formatter.format_help())


class DBTArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register('action', 'dbtversion', DBTVersion)

    def add_optional_argument_inverse(
        self,
        name,
        *,
        enable_help=None,
        disable_help=None,
        dest=None,
        no_name=None,
        default=None,
    ):
        mutex_group = self.add_mutually_exclusive_group()
        if not name.startswith('--'):
            raise InternalException(
                'cannot handle optional argument without "--" prefix: '
                f'got "{name}"'
            )
        if dest is None:
            dest_name = name[2:].replace('-', '_')
        else:
            dest_name = dest

        if no_name is None:
            no_name = f'--no-{name[2:]}'

        mutex_group.add_argument(
            name,
            action='store_const',
            const=True,
            dest=dest_name,
            default=default,
            help=enable_help,
        )

        mutex_group.add_argument(
            f'--no-{name[2:]}',
            action='store_const',
            const=False,
            dest=dest_name,
            default=default,
            help=disable_help,
        )

        return mutex_group


class RPCArgumentParser(DBTArgumentParser):
    def exit(self, status=0, message=None):
        if status == 0:
            return
        else:
            raise TypeError(message)


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


# here for backwards compatibility
def handle(args):
    res, success = handle_and_check(args)
    return res


@contextmanager
def adapter_management():
    reset_adapters()
    try:
        yield
    finally:
        cleanup_connections()


def handle_and_check(args):
    with log_manager.applicationbound():
        parsed = parse_args(args)

        # Set flags from args, user config, and env vars
        user_config = read_user_config(flags.PROFILES_DIR)  # This is read again later
        flags.set_from_args(parsed, user_config)
        initialize_tracking_from_flags()
        # Set log_format from flags
        parsed.cls.set_log_format()

        # we've parsed the args and set the flags - we can now decide if we're debug or not
        if flags.DEBUG:
            log_manager.set_debug()

        profiler_enabled = False

        if parsed.record_timing_info:
            profiler_enabled = True

        with profiler(
            enable=profiler_enabled,
            outfile=parsed.record_timing_info
        ):

            with adapter_management():

                task, res = run_from_args(parsed)
                success = task.interpret_results(res)

            return res, success


@contextmanager
def track_run(task):
    dbt.tracking.track_invocation_start(config=task.config, args=task.args)
    try:
        yield
        dbt.tracking.track_invocation_end(
            config=task.config, args=task.args, result_type="ok"
        )
    except (NotImplementedException,
            FailedToConnectException) as e:
        logger.error('ERROR: {}'.format(e))
        dbt.tracking.track_invocation_end(
            config=task.config, args=task.args, result_type="error"
        )
    except Exception:
        dbt.tracking.track_invocation_end(
            config=task.config, args=task.args, result_type="error"
        )
        raise
    finally:
        dbt.tracking.flush()


def run_from_args(parsed):
    log_cache_events(getattr(parsed, 'log_cache_events', False))

    # we can now use the logger for stdout
    # set log_format in the logger
    parsed.cls.pre_init_hook(parsed)

    logger.info("Running with dbt{}".format(dbt.version.installed))

    # this will convert DbtConfigErrors into RuntimeExceptions
    # task could be any one of the task objects
    task = parsed.cls.from_args(args=parsed)

    logger.debug("running dbt with arguments {parsed}", parsed=str(parsed))

    log_path = None
    if task.config is not None:
        log_path = getattr(task.config, 'log_path', None)
    # we can finally set the file logger up
    log_manager.set_path(log_path)
    if dbt.tracking.active_user is not None:  # mypy appeasement, always true
        logger.debug("Tracking: {}".format(dbt.tracking.active_user.state()))

    results = None

    with track_run(task):
        results = task.run()

    return task, results


def _build_base_subparser():
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        '--project-dir',
        default=None,
        type=str,
        help='''
        Which directory to look in for the dbt_project.yml file.
        Default is the current working directory and its parents.
        '''
    )

    base_subparser.add_argument(
        '--profiles-dir',
        default=None,
        dest='sub_profiles_dir',  # Main cli arg precedes subcommand
        type=str,
        help='''
        Which directory to look in for the profiles.yml file. Default = {}
        '''.format(DEFAULT_PROFILES_DIR)
    )

    base_subparser.add_argument(
        '--profile',
        required=False,
        type=str,
        help='''
        Which profile to load. Overrides setting in dbt_project.yml.
        '''
    )

    base_subparser.add_argument(
        '-t',
        '--target',
        default=None,
        type=str,
        help='''
        Which target to load for the given profile
        ''',
    )

    base_subparser.add_argument(
        '--vars',
        type=str,
        default='{}',
        help='''
        Supply variables to the project. This argument overrides variables
        defined in your dbt_project.yml file. This argument should be a YAML
        string, eg. '{my_variable: my_value}'
        '''
    )

    # if set, log all cache events. This is extremely verbose!
    base_subparser.add_argument(
        '--log-cache-events',
        action='store_true',
        help=argparse.SUPPRESS,
    )

    base_subparser.set_defaults(defer=None, state=None)
    return base_subparser


def parse_args(args, cls=DBTArgumentParser):
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
        default=None,
        help='''
        Display debug logging during dbt execution. Useful for debugging and
        making bug reports.
        '''
    )

    p.add_argument(
        '--log-format',
        choices=['text', 'json', 'default'],
        default=None,
        help='''Specify the log format, overriding the command's default.'''
    )

    p.add_argument(
        '--no-write-json',
        action='store_false',
        default=None,
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
        default=None,
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
        '--printer-width',
        dest='printer_width',
        help='''
        Sets the width of terminal output
        '''
    )

    p.add_argument(
        '--warn-error',
        action='store_true',
        default=None,
        help='''
        If dbt would normally warn, instead raise an exception. Examples
        include --models that selects nothing, deprecations, configurations
        with no associated models, invalid test configurations, and missing
        sources/refs in tests.
        '''
    )

    p.add_argument(
        '--no-version-check',
        dest='version_check',
        action='store_false',
        default=None,
        help='''
        If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
        '''
    )

    p.add_optional_argument_inverse(
        '--partial-parse',
        enable_help='''
        Allow for partial parsing by looking for and writing to a pickle file
        in the target directory. This overrides the user configuration file.
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

    # if set, will use the latest features from the static parser instead of
    # the stable static parser.
    p.add_argument(
        '--use-experimental-parser',
        action='store_true',
        default=None,
        help='''
        Enables experimental parsing features.
        '''
    )

    # if set, will disable the use of the stable static parser and instead
    # always rely on jinja rendering.
    p.add_argument(
        '--no-static-parser',
        default=None,
        dest='static_parser',
        action='store_false',
        help='''
        Disables the static parser.
        '''
    )

    p.add_argument(
        '--profiles-dir',
        default=None,
        dest='profiles_dir',
        type=str,
        help='''
        Which directory to look in for the profiles.yml file. Default = {}
        '''.format(DEFAULT_PROFILES_DIR)
    )

    p.add_argument(
        '--no-anonymous-usage-stats',
        action='store_false',
        default=None,
        dest='send_anonymous_usage_stats',
        help='''
        Do not send anonymous usage stat to dbt Labs
        '''
    )

    p.add_argument(
        '-x',
        '--fail-fast',
        dest='fail_fast',
        action='store_true',
        default=None,
        help='''
        Stop execution upon a first failure.
        '''
    )

    subs = p.add_subparsers(title="Available sub-commands")

    base_subparser = _build_base_subparser()

    rpc_sub = subs.add_parser(
        'serve',
        parents=[base_subparser],
        help='''
        Start a json-rpc server
        ''',
    )
    rpc_sub.add_argument(
        '--host',
        default='0.0.0.0',
        help='''
        Specify the host to listen on for the rpc server.
        ''',
    )
    rpc_sub.add_argument(
        '--port',
        default=8580,
        type=int,
        help='''
        Specify the port number for the rpc server.
        ''',
    )
    rpc_sub.set_defaults(cls=RPCServerTask, which='rpc', rpc_method=None)
    # the rpc task does a 'compile', so we need these attributes to exist, but
    # we don't want users to be allowed to set them.
    rpc_sub.set_defaults(models=None, exclude=None)

    rpc_sub.add_argument(
        '--threads',
        type=int,
        required=False,
        help='''
        Specify number of threads to use while executing models. Overrides
        settings in profiles.yml.
        '''
    )
    rpc_sub.add_argument(
        '--no-version-check',
        dest='sub_version_check',  # main cli arg precedes subcommands
        action='store_false',
        default=None,
        help='''
        If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
        '''
    )

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    # profiles_dir is set before subcommands and after, so normalize
    if hasattr(parsed, 'sub_profiles_dir'):
        if parsed.sub_profiles_dir is not None:
            parsed.profiles_dir = parsed.sub_profiles_dir
        delattr(parsed, 'sub_profiles_dir')
    if hasattr(parsed, 'profiles_dir'):
        if parsed.profiles_dir is None:
            parsed.profiles_dir = flags.PROFILES_DIR
        else:
            parsed.profiles_dir = os.path.abspath(parsed.profiles_dir)
            # needs to be set before the other flags, because it's needed to
            # read the profile that contains them
            flags.PROFILES_DIR = parsed.profiles_dir

    # version_check is set before subcommands and after, so normalize
    if hasattr(parsed, 'sub_version_check'):
        if parsed.sub_version_check is False:
            parsed.version_check = False
        delattr(parsed, 'sub_version_check')

    # fail_fast is set before subcommands and after, so normalize
    if hasattr(parsed, 'sub_fail_fast'):
        if parsed.sub_fail_fast is True:
            parsed.fail_fast = True
        delattr(parsed, 'sub_fail_fast')

    if getattr(parsed, 'project_dir', None) is not None:
        expanded_user = os.path.expanduser(parsed.project_dir)
        parsed.project_dir = os.path.abspath(expanded_user)

    if not hasattr(parsed, 'which'):
        # the user did not provide a valid subcommand. trigger the help message
        # and exit with a error
        p.print_help()
        p.exit(1)

    return parsed
