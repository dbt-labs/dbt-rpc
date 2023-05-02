import pytest

from .util import (
    get_querier,
    ProjectDefinition,
)


@pytest.mark.supported("any")
def test_rpc_reload(project_root, profiles_root, dbt_profile, unique_schema):
    project = ProjectDefinition(models={"my_model.sql": "select 1 as id"})
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
    )
    with querier_ctx as querier:
        querier.request(method="reload")
