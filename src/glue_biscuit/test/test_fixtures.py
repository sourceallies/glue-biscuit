import pytest
from awsglue.context import GlueContext
from unittest.mock import Mock


def test_mock_glue_context_is_correct_type(mock_glue_context: GlueContext):
    assert isinstance(mock_glue_context, GlueContext)


mocked_methods = [
    "create_dynamic_frame_from_catalog",
    "create_dynamic_frame_from_options",
    "getSource",
    "create_data_frame_from_catalog",
    "create_data_frame_from_options",
    "forEachBatch",
    "purge_table",
    "purge_s3_path",
    "getSink",
    "write_dynamic_frame_from_options",
    "write_from_options",
    "write_dynamic_frame_from_catalog",
    "write_dynamic_frame_from_jdbc_conf",
    "write_from_jdbc_conf",
]


@pytest.mark.parametrize(
    "method_name",
    mocked_methods,
)
def test_mock_glue_context_mocks_method(
    method_name: str, mock_glue_context: GlueContext
):
    mock_method = getattr(mock_glue_context, method_name)
    assert isinstance(mock_method, Mock)
    assert mock_method.return_value is None, "Mock not set to reuturn None"
