from dbt.contracts.results import (
    RunResult,
    RunOperationResult,
    FreshnessResult,
)
from dbt_rpc.contracts.rpc import (
    RemoteExecutionResult,
    RemoteFreshnessResult,
    RemoteRunOperationResult,
)
from dbt.task.runnable import GraphRunnableTask
from dbt_rpc.rpc.method import RemoteManifestMethod, Parameters
from typing import AbstractSet


RESULT_TYPE_MAP = {
    RunResult: RemoteExecutionResult,
    RunOperationResult: RemoteRunOperationResult,
    FreshnessResult: RemoteFreshnessResult,
}


class RPCTask(
    GraphRunnableTask,
    RemoteManifestMethod[Parameters, RemoteExecutionResult]
):
    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        RemoteManifestMethod.__init__(
            self, args, config, manifest  # type: ignore
        )

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def defer_to_manifest(self, adapter, selected_uids: AbstractSet[str]):
        # https://github.com/dbt-labs/dbt-core/pull/6488
        # abstract method added to GraphRunnableTask
        # bacause of how we do multiple inheritance for project commands,
        # this will be overridden by task-specific methods
        pass

    def get_result(
        self, results, elapsed_time, generated_at
    ) -> RemoteExecutionResult:
        base = super().get_result(results, elapsed_time, generated_at)
        cls = RESULT_TYPE_MAP.get(type(base), RemoteExecutionResult)
        rpc_result = cls.from_local_result(base, logs=[])
        return rpc_result
