import re
import pytest

from .util import (
    get_querier,
    ProjectDefinition,
)


@pytest.mark.supported('any')
def test_rpc_run_sql_nohang(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'}
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
    )
    with querier_ctx as querier:
        result = querier.async_wait_for_result(querier.compile_sql('select 1 as id', language='python'))
        assert 'dbt_load_df_function' in result['results'][0]['compiled_sql']

@pytest.mark.supported('any')
def test_rpc_compile_macro(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'}
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
    )
    with querier_ctx as querier:
        result = querier.async_wait_for_result(querier.compile_sql('{% macro test()%}\n 1\n{%endmacro%}\n {{ test() }}', ))
        assert '\n \n 1\n' == result['results'][0]['compiled_sql']

