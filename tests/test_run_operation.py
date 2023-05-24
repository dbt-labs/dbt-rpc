import pytest

from .util import (
    get_querier,
    ProjectDefinition,
)

macros_data = '''
{% macro foo() %}
    {{ return(1) }}
{% endmacro %}
{% macro bar(value) %}
    {{ return(value + 1) }}
{% endmacro %}
{% macro quux(value) %}
    {{ return(asdf) }}
{% endmacro %}
'''

def _check_success(result):
    assert 'state' in result
    assert result['state'] == 'success'

@pytest.mark.supported('postgres')
def test_run_operation(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'},
        macros={
            'my_macros.sql': macros_data,
        }
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
    )

    with querier_ctx as querier:
        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='foo', args={})
        )

        _check_success(poll_result)

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='bar', args={'value': 10})
        )

        _check_success(poll_result)

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='baz', args={}),
            state='failed',
        )
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='quux', args={})
        )
        _check_success(poll_result)


@pytest.mark.supported('postgres')
def test_run_operation_cli(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'},
        macros={
            'my_macros.sql': macros_data,
        }
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
    )

    with querier_ctx as querier:
        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation foo')
        )
        _check_success(poll_result)

        bar_cmd = '''run-operation bar --args="{'value': 10}"'''
        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli=bar_cmd)
        )

        _check_success(poll_result)

        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation baz'),
            state='failed',
        )
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation quux')
        )
        _check_success(poll_result)

