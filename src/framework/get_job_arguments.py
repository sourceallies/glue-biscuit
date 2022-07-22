from typing import Tuple
from awsglue.utils import getResolvedOptions
import sys

__auto_included_args = {
    "JOB_ID",
    "JOB_RUN_ID",
    "SECURITY_CONFIGURATION",
    "encryption_type",
    "continuation_option",
    "job_bookmark_option",
}


def get_job_arguments(*arg_names: str) -> Tuple[str]:
    """
    Takes a variable array of argument names. Looks up these arguments and returns them in a tuple in that order
    """
    args_to_resolve = [arg for arg in arg_names if arg not in __auto_included_args]
    resolved_options = getResolvedOptions(sys.argv, args_to_resolve)
    print(resolved_options)
    return tuple(resolved_options[arg] for arg in arg_names)


def get_job_argument(arg_name: str) -> str:
    """
    Convinence wrapper around get_job_arguments for getting a single argument
    """
    result, = get_job_arguments(arg_name)
    return result