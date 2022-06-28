from typing import Tuple
from awsglue.utils import getResolvedOptions
import sys

__auto_included_args = {
    'JOB_ID', 
    'JOB_RUN_ID', 
    'SECURITY_CONFIGURATION',
    'encryption_type',
    'continuation_option',
    'job_bookmark_option'
}

def get_job_arguments(*arg_names: str) -> Tuple[str]:
    args_to_resolve = [arg for arg in arg_names if arg not in __auto_included_args]
    resolved_options = getResolvedOptions(sys.argv, args_to_resolve)
    print(resolved_options)
    return tuple(resolved_options[arg] for arg in arg_names)