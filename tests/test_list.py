import os
import pytest
from .util import (
    assert_has_threads,
    get_querier,
    get_write_manifest,
    ProjectDefinition,
)


@pytest.mark.supported('postgres')
def test_rpc_run_threads(
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
        # run a run so that we have a request token
        run_result = querier.run()
        # run a list command so we have a task going
        result = querier.list()
        # there's code added in poll to swap out the request token so we are actually
        # getting the list command result back
        result1 = querier.poll(run_result['result']['request_token'])
        assert 'error' not in result1

        # this runs the poll loop
        # querier.async_wait_for_result(
        #     result
        # )

