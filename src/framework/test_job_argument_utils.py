from unittest.mock import patch
import pytest
import sys
from framework.job_argument_utils import get_job_arguments, get_job_argument


@pytest.fixture(autouse=True)
def mock_argv():
    mock_args = [
        # The first itme has to be the name of the script
        "script_name.py",
        "--foo",
        "some_value",
        "--bar",
        "value_2",
        "--JOB_ID",
        "some_job_id",
        "--JOB_RUN_ID",
        "run_id",
        "--SECURITY_CONFIGURATION",
        "config",
        "--continuation-option",
        "continuation-enabled",
        "--encryption-type",
        "sse-s3",
    ]
    with patch.object(sys, "argv", mock_args):
        yield mock_args


def test_one_argument_returned_if_passed():
    (result,) = get_job_arguments("foo")
    assert result == "some_value"


def test_multiple_arguments_returned():
    foo, bar = get_job_arguments("foo", "bar")
    assert foo == "some_value"
    assert bar == "value_2"


def test_multiple_arguments_returned_in_order():
    bar, foo = get_job_arguments("bar", "foo")
    assert foo == "some_value"
    assert bar == "value_2"


@pytest.mark.parametrize(
    ["key", "value"],
    [
        ("JOB_ID", "some_job_id"),
        ("JOB_RUN_ID", "run_id"),
        ("SECURITY_CONFIGURATION", "config"),
        ("encryption_type", "sse-s3"),
    ],
)
def test_built_int_args_are_resolvable(key, value):
    (result,) = get_job_arguments(key)
    assert result == value


@patch("framework.job_argument_utils.get_job_arguments")
def test_get_job_argument_convinence_function_translates(mock_get_job_arguments):
    mock_get_job_arguments.return_value = ("a",)

    (result,) = get_job_argument("arg_name")

    mock_get_job_arguments.assert_called_with("arg_name")
    assert result == "a"
