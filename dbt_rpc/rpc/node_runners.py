from abc import abstractmethod
from datetime import datetime
from typing import Generic, TypeVar

import dbt.exceptions
from dbt_rpc.contracts.rpc import (
    RemoteCompileResult,
    RemoteCompileResultMixin,
    RemoteRunResult,
    ResultTable,
)
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.compile import CompileRunner
from dbt_rpc.rpc.error import dbt_error, RPCException, server_error


RPCSQLResult = TypeVar("RPCSQLResult", bound=RemoteCompileResultMixin)


def get_raw_and_compiled(compiled_node):
    compiled_prev = getattr(compiled_node, 'compiled_sql', None)
    compiled_new = getattr(compiled_node, 'compiled_code', None)
    compiled_sql = compiled_new or compiled_prev
    raw_prev = getattr(compiled_node, 'raw_sql', None)
    raw_new = getattr(compiled_node, 'raw_code', None)
    raw_sql = raw_new or raw_prev
    return raw_sql, compiled_sql


class GenericRPCRunner(CompileRunner, Generic[RPCSQLResult]):
    def __init__(self, config, adapter, node, node_index, num_nodes):
        CompileRunner.__init__(self, config, adapter, node, node_index, num_nodes)

    def handle_exception(self, e, ctx):
        logger.debug("Got an exception: {}".format(e), exc_info=True)
        if isinstance(e, dbt.exceptions.Exception):
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                e.add_node(ctx.node)
            return dbt_error(e)
        elif isinstance(e, RPCException):
            return e
        else:
            return server_error(e)

    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def compile(self, manifest):
        if not self.node.config.enabled:
            raise dbt.exceptions.CompilationError(
                "Trying to compile a node that is disabled",
                None
            )
        compiler = self.adapter.get_compiler()
        return compiler.compile_node(self.node, manifest, {}, write=False)

    @abstractmethod
    def execute(self, compiled_node, manifest) -> RPCSQLResult:
        pass

    @abstractmethod
    def from_run_result(self, result, start_time, timing_info) -> RPCSQLResult:
        pass

    def error_result(self, node, error, start_time, timing_info):
        raise error

    def ephemeral_result(self, node, start_time, timing_info):
        raise dbt.exceptions.NotImplementedException(
            "cannot execute ephemeral nodes remotely!"
        )


class RPCCompileRunner(GenericRPCRunner[RemoteCompileResult]):
    def execute(self, compiled_node, manifest) -> RemoteCompileResult:
        raw_sql, compiled_sql = get_raw_and_compiled(compiled_node)
        return RemoteCompileResult(
            raw_sql=raw_sql,
            compiled_sql=compiled_sql,
            node=compiled_node,
            timing=[],  # this will get added later
            logs=[],
            generated_at=datetime.utcnow(),
        )

    def from_run_result(
            self,
            result: RemoteRunResult,
            start_time,
            timing_info
    ) -> RemoteCompileResult:

        return RemoteCompileResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            timing=timing_info,
            logs=[],
            generated_at=datetime.utcnow(),
        )


class RPCExecuteRunner(GenericRPCRunner[RemoteRunResult]):
    def execute(self, compiled_node, manifest) -> RemoteRunResult:
        raw_sql, compiled_sql = get_raw_and_compiled(compiled_node)
        _, execute_result = self.adapter.execute(compiled_sql, fetch=True)

        table = ResultTable(
            column_names=list(execute_result.column_names),
            rows=[list(row) for row in execute_result],
        )

        return RemoteRunResult(
            raw_sql=raw_sql,
            compiled_sql=compiled_sql,
            node=compiled_node,
            table=table,
            timing=[],
            logs=[],
            generated_at=datetime.utcnow(),
        )

    def from_run_result(self, result: RemoteRunResult, start_time, timing_info) -> RemoteRunResult:
        return RemoteRunResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            table=result.table,
            timing=timing_info,
            logs=[],
            generated_at=datetime.utcnow(),
        )
