import pytest
from awsglue.context import GlueContext
from unittest.mock import Mock

from framework.fixtures import spark_context, mock_glue_context


def test_mock_glue_context_is_correct_type(mock_glue_context: GlueContext):
    assert isinstance(mock_glue_context, GlueContext)


@pytest.mark.parametrize('method_name', [
    'create_dynamic_frame_from_catalog',
    'create_dynamic_frame_from_options',
    'create_sample_dynamic_frame_from_catalog',
    'create_sample_dynamic_frame_from_options',
    'getSource',
    'create_data_frame_from_catalog',
    'create_data_frame_from_options',
    'forEachBatch',
    'purge_table',
    'purge_s3_path',
    'getSink',
    'write_dynamic_frame_from_options',
    'write_from_options',
    'write_dynamic_frame_from_catalog',
    'write_dynamic_frame_from_jdbc_conf',
    'write_from_jdbc_conf',
])
def test_mock_glue_context_mocks_method(method_name: str, mock_glue_context: GlueContext):
    assert isinstance(mock_glue_context.__dict__[method_name], Mock)
